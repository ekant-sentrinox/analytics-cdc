package ai.sentrinox;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.StartupScriptProvider;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * Change-data-capture job for the analytics reference (dimension) tables.
 *
 * <p>Reads every {@code (customer_id, tenant_id)} pair from
 * {@code customer_tenant_reference} (the V1 table, stored in the DuckLake
 * catalog backed by MinIO), then calls the SCCAL internal-sync API
 * ({@code GET /internal/sccal/api/v1/{objectType}/list}) for each pair and
 * captures the changes into the V6 reference tables.
 *
 * <p>Capture is snapshot-based: for each target table the current API snapshot
 * (union-ed across all pairs and de-duplicated by key) is diffed against the
 * table and the difference is applied as INSERT (new), UPDATE (renamed) and
 * DELETE (absent / {@code isDeleted:true}) — the three CDC operations. The API
 * is queried without {@code commitId}, so every response is a full snapshot and
 * "absent from snapshot" is a safe delete signal. All table merges run in one
 * transaction, so the capture lands as a single atomic DuckLake snapshot; the
 * job is also idempotent, so a re-run converges.
 *
 * <p>JSON is parsed by DuckDB itself (the raw response is bound as a {@code JSON}
 * parameter and shredded with {@code json_extract(...,'$.*')}), so the job needs
 * no JSON library beyond the DuckDB driver already on the classpath.
 *
 * <p>With {@code analytics_cdc.poll_interval = 0} (default) it runs one capture
 * and exits. A positive interval keeps the process alive and re-captures on that
 * interval, reusing the warm DuckDB connection and surviving per-cycle failures.
 *
 * <p>Run with: {@code java -cp analytics-cdc.jar ai.sentrinox.SccalReferenceSync}
 */
public final class SccalReferenceSync {

    private static final String API_PATH = "/internal/sccal/api/v1/";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Object types that ignore tenant scoping — fetched once (with the first
     * pair's credentials), not per pair. Only types that actually appear in
     * {@link #SYNCS} belong here; an entry with no matching Sync is silently
     * dropped at fetch time and only misleads the reader.
     *
     * <p>NOTE: "fetch once" assumes the API returns the SAME global snapshot
     * regardless of the calling customer/tenant. If the API ever auth-trims a
     * "global" response per caller, rows visible only to other pairs would be
     * absent from the snapshot and treated as deletes — move such a type out of
     * this set so it is fetched per pair and union-ed instead.
     */
    private static final Set<String> GLOBAL_TYPES = Set.of("providercatalogue");

    /** One column of a reference table and how to pull it out of an entity. */
    private record Col(String column, String jsonField, String sqlType) {
        /** DuckDB expression extracting + casting this column from entity alias {@code e}. */
        String expr() {
            return "(e->>'" + jsonField + "')::" + sqlType;
        }
    }

    /**
     * A reference table fed from one SCCAL object type, with the SQL needed to
     * stage and capture it.
     *
     * @param objectType API path segment ({@code user}, {@code rule}, ...)
     * @param table      target table, already schema-qualified and quoted
     * @param keyCols    identity columns (the CDC key)
     * @param dataCols   non-key attribute columns (e.g. name); empty for edge tables
     * @param filter     optional extra predicate on the staged entity (alias {@code e})
     */
    record Sync(String objectType, String table, List<Col> keyCols,
                List<Col> dataCols, String filter) {

        private List<Col> allCols() {
            List<Col> all = new ArrayList<>(keyCols);
            all.addAll(dataCols);
            return all;
        }

        /** Temp staging table name, unique per target table. */
        private String stage() {
            return "cdc_stg_" + table.replaceAll("[^A-Za-z0-9]", "_");
        }

        /** {@code t.k1 = s.k1 AND t.k2 = s.k2} for the staging-vs-target key match. */
        private String keyEq() {
            return join(keyCols, c -> "t." + c.column() + " = s." + c.column(), " AND ");
        }

        private String createTempSql() {
            return "CREATE TEMP TABLE " + stage() + " ("
                + join(allCols(), c -> c.column() + " " + c.sqlType(), ", ") + ")";
        }

        /** Parameterised: bind the raw JSON response as parameter 1. */
        String stageSql() {
            String sql = "INSERT INTO " + stage() + " (" + columns(allCols()) + ") SELECT "
                + join(allCols(), Col::expr, ", ")
                + " FROM (SELECT unnest(json_extract(?::JSON, '$.*')) AS e)"
                // isDeleted is omitempty; treat missing as active, true as a delete.
                + " WHERE coalesce((e->>'isDeleted')::BOOLEAN, false) = false";
            return filter == null ? sql : sql + " AND " + filter;
        }

        /** Collapse duplicate keys (e.g. a global type staged from several pairs). */
        String dedupeSql() {
            String keys = columns(keyCols);
            // Order by ALL columns, not the partition keys: ordering by the keys
            // alone leaves every row in a partition tied, so row_number() = 1 keeps
            // an arbitrary row and the captured name flaps between runs (breaking
            // idempotent convergence) when the same key arrives with differing data.
            // Ordering by all columns makes the surviving row deterministic.
            String order = columns(allCols());
            return "CREATE OR REPLACE TEMP TABLE " + stage() + " AS SELECT * FROM " + stage()
                + " QUALIFY row_number() OVER (PARTITION BY " + keys + " ORDER BY " + order + ") = 1";
        }

        String deleteSql() {
            return "DELETE FROM " + table + " t WHERE NOT EXISTS "
                + "(SELECT 1 FROM " + stage() + " s WHERE " + keyEq() + ")";
        }

        String updateSql() {
            String set = join(dataCols, c -> c.column() + " = s." + c.column(), ", ");
            return "UPDATE " + table + " t SET " + set + " FROM " + stage() + " s WHERE "
                + keyEq() + " AND (" + changedPredicate() + ")";
        }

        String insertSql() {
            String cols = columns(allCols());
            return "INSERT INTO " + table + " (" + cols + ") SELECT " + cols + " FROM " + stage()
                + " s WHERE NOT EXISTS (SELECT 1 FROM " + table + " t WHERE " + keyEq() + ")";
        }

        private String changedPredicate() {
            return join(dataCols, c -> "t." + c.column() + " IS DISTINCT FROM s." + c.column(), " OR ");
        }

        /** Apply DELETE / UPDATE / INSERT; returns {inserted, updated, deleted}. */
        private int[] merge(Statement st) throws SQLException {
            // An empty staging table is ambiguous: it can mean "this type genuinely
            // has no rows" but also "the API returned a blank/degenerate-but-200
            // body" (coalesced to "{}") or "every pair failed". Treating that as a
            // delete would wipe the whole target table on a transient hiccup, so
            // skip DELETE when staging is empty and leave the existing rows intact.
            int deleted = stageIsEmpty(st) ? 0 : st.executeUpdate(deleteSql());
            int updated = dataCols.isEmpty() ? 0 : st.executeUpdate(updateSql());
            int inserted = st.executeUpdate(insertSql());
            return new int[] {inserted, updated, deleted};
        }

        private boolean stageIsEmpty(Statement st) throws SQLException {
            try (ResultSet rs = st.executeQuery("SELECT 1 FROM " + stage() + " LIMIT 1")) {
                return !rs.next();
            }
        }

        private String displayName() {
            int dot = table.lastIndexOf('.');
            return (dot < 0 ? table : table.substring(dot + 1)).replace("\"", "");
        }
    }

    // The seven reference tables and the object type each is captured from.
    // RULE feeds two tables, split by ruleType (1 = provider/LLM, 3 = MCP).
    static final List<Sync> SYNCS = List.of(
        new Sync("workspace", "ollylake.main.workspace",
            List.of(new Col("workspace_id", "workspaceId", "BIGINT")),
            List.of(new Col("name", "name", "VARCHAR")), null),
        new Sync("user", "ollylake.main.\"user\"",
            List.of(new Col("user_id", "userId", "BIGINT")),
            List.of(new Col("name", "userName", "VARCHAR")), null),
        new Sync("usergroup", "ollylake.main.\"group\"",
            List.of(new Col("group_id", "userGroupId", "BIGINT")),
            List.of(new Col("name", "name", "VARCHAR")), null),
        new Sync("usergroupmembership", "ollylake.main.user_group_mapping",
            List.of(new Col("user_id", "userId", "BIGINT"),
                    new Col("group_id", "userGroupId", "BIGINT")),
            List.of(), null),
        new Sync("providercatalogue", "ollylake.main.provider",
            List.of(new Col("provider_id", "type", "INTEGER")),
            List.of(new Col("name", "name", "VARCHAR")), null),
        new Sync("rule", "ollylake.main.llm_access_rule",
            List.of(new Col("llm_access_rule_id", "ruleId", "BIGINT")),
            List.of(new Col("name", "name", "VARCHAR")), "(e->>'ruleType')::INTEGER = 1"),
        new Sync("rule", "ollylake.main.mcp_access_rule",
            List.of(new Col("mcp_access_rule_id", "ruleId", "BIGINT")),
            List.of(new Col("name", "name", "VARCHAR")), "(e->>'ruleType')::INTEGER = 3")
    );

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load().getConfig("analytics_cdc");
        String baseUrl = trimSlash(config.getString("sccal_base_url"));
        Duration pollInterval = config.getDuration("poll_interval");
        // INSTALL/LOAD extensions, CREATE SECRET and ATTACH the DuckLake catalog.
        String startupScript = StartupScriptProvider.load(config).getStartupScript();

        System.out.println("SCCAL reference sync — API: " + baseUrl);

        HttpClient http = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = conn.createStatement()) {

            // Bootstrap once; the warm connection is reused across every cycle.
            for (String stmt : splitStatements(startupScript)) {
                st.execute(stmt);
            }

            if (isOneShot(pollInterval)) {
                runOnce(conn, st, http, baseUrl);          // one-shot (default)
                System.out.println("\nSCCAL reference sync complete.");
            } else {
                poll(conn, st, http, baseUrl, pollInterval);
            }
        }
    }

    /** A zero or negative {@code poll_interval} means run once and exit (the default). */
    static boolean isOneShot(Duration pollInterval) {
        return pollInterval.isZero() || pollInterval.isNegative();
    }

    /** Repeatedly capture on a fixed interval, surviving per-cycle failures. */
    private static void poll(Connection conn, Statement st, HttpClient http,
                             String baseUrl, Duration interval) throws InterruptedException {
        System.out.println("Polling every " + interval.toSeconds() + "s (Ctrl-C to stop).");
        while (true) {
            System.out.println("\n--- sync @ " + LocalTime.now().withNano(0) + " ---");
            pollCycle(conn, st, http, baseUrl);
            Thread.sleep(interval.toMillis());
        }
    }

    /**
     * Run one poll cycle. A per-cycle failure (API down, transient DB error) is
     * logged and the connection reset — never propagated — so the poll loop
     * survives it and the next cycle starts from clean connection state.
     */
    static void pollCycle(Connection conn, Statement st, HttpClient http, String baseUrl) {
        try {
            runOnce(conn, st, http, baseUrl);
        } catch (Exception e) {
            System.err.println("sync cycle failed: " + e + " — retrying next interval");
            // The failure (or a throw from applyCapture's finally) may have left
            // the connection mid-transaction with autoCommit still false. Reset it
            // so this stale state can't leak into the next cycle's staging writes.
            resetConnection(conn);
        }
    }

    /** Best-effort: abandon any open transaction and restore autoCommit after a failed cycle. */
    static void resetConnection(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // Connection was already in autoCommit mode (or unusable) — nothing to roll back.
        }
        try {
            conn.setAutoCommit(true);
        } catch (SQLException ignored) {
            // Nothing more we can do here; the next cycle will surface a hard failure.
        }
    }

    /** One full capture pass: read pairs, fetch + stage, then merge. */
    static void runOnce(Connection conn, Statement st, HttpClient http,
                        String baseUrl) throws SQLException {
        List<long[]> pairs = readCustomerTenantPairs(st);
        if (pairs.isEmpty()) {
            System.out.println("No (customerId, tenantId) pairs in "
                + "customer_tenant_reference — nothing to capture.");
            return;
        }
        stageAll(conn, st, http, baseUrl, pairs);
        applyCapture(conn, st);
    }

    /** Fetch every object type and stage its rows; globals once, tenant types per pair. */
    private static void stageAll(Connection conn, Statement st, HttpClient http,
                                 String baseUrl, List<long[]> pairs) throws SQLException {
        // objectType -> the sync(s) it feeds (RULE feeds two), preserving declared order.
        Map<String, List<Sync>> byType = new LinkedHashMap<>();
        for (Sync s : SYNCS) {
            byType.computeIfAbsent(s.objectType(), k -> new ArrayList<>()).add(s);
        }
        List<String> globalTypes = byType.keySet().stream().filter(GLOBAL_TYPES::contains).toList();
        List<String> tenantTypes = byType.keySet().stream().filter(t -> !GLOBAL_TYPES.contains(t)).toList();

        for (Sync s : SYNCS) {
            st.execute("DROP TABLE IF EXISTS " + s.stage());
            st.execute(s.createTempSql());
        }

        // Global types ignore tenant scoping — fetch once (params still required).
        if (!globalTypes.isEmpty()) {
            long[] any = pairs.get(0);
            stageResponses(conn, byType, fetchAll(http, baseUrl, globalTypes, any[0], any[1]));
        }
        // Tenant-scoped types — fetched concurrently for each pair.
        for (long[] pair : pairs) {
            stageResponses(conn, byType, fetchAll(http, baseUrl, tenantTypes, pair[0], pair[1]));
        }
    }

    private static void stageResponses(Connection conn, Map<String, List<Sync>> byType,
                                       Map<String, String> responses) throws SQLException {
        for (Map.Entry<String, String> e : responses.entrySet()) {
            for (Sync s : byType.get(e.getKey())) {
                stage(conn, s, e.getValue());
            }
        }
    }

    /** De-dupe staging, then merge every table inside one transaction (atomic snapshot). */
    private static void applyCapture(Connection conn, Statement st) throws SQLException {
        for (Sync s : SYNCS) {
            st.execute(s.dedupeSql());
        }

        System.out.printf("%n%-28s %7s %7s %7s%n", "table", "ins", "upd", "del");
        System.out.println("-".repeat(52));

        conn.setAutoCommit(false);
        try {
            int totalIns = 0, totalUpd = 0, totalDel = 0;
            for (Sync s : SYNCS) {
                int[] c = s.merge(st);
                totalIns += c[0];
                totalUpd += c[1];
                totalDel += c[2];
                System.out.printf("%-28s %7d %7d %7d%n", s.displayName(), c[0], c[1], c[2]);
            }
            // Nothing changed → roll back so we don't write an empty DuckLake snapshot.
            if (totalIns + totalUpd + totalDel > 0) {
                conn.commit();
            } else {
                conn.rollback();
            }
            System.out.println("-".repeat(52));
            System.out.printf("%-28s %7d %7d %7d%n", "TOTAL", totalIns, totalUpd, totalDel);
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
            for (Sync s : SYNCS) {
                st.execute("DROP TABLE IF EXISTS " + s.stage());
            }
        }
    }

    // ---- API ----------------------------------------------------------------

    /** Issue all requests concurrently, then collect the bodies (keyed by object type). */
    static Map<String, String> fetchAll(HttpClient http, String baseUrl,
                                         List<String> types, long customerId, long tenantId) {
        Map<String, CompletableFuture<HttpResponse<String>>> inflight = new LinkedHashMap<>();
        for (String type : types) {
            inflight.put(type, http.sendAsync(
                request(baseUrl, type, customerId, tenantId), HttpResponse.BodyHandlers.ofString()));
        }
        Map<String, String> bodies = new LinkedHashMap<>();
        for (Map.Entry<String, CompletableFuture<HttpResponse<String>>> e : inflight.entrySet()) {
            HttpResponse<String> resp;
            try {
                resp = e.getValue().join();
            } catch (CompletionException ce) {
                throw new RuntimeException("fetch failed for " + e.getKey()
                    + " (customerId=" + customerId + ", tenantId=" + tenantId + ")", ce.getCause());
            }
            if (resp.statusCode() != 200) {
                throw new IllegalStateException("GET " + resp.uri() + " -> HTTP " + resp.statusCode());
            }
            String body = resp.body();
            bodies.put(e.getKey(), (body == null || body.isBlank()) ? "{}" : body);
        }
        return bodies;
    }

    private static HttpRequest request(String baseUrl, String objectType,
                                       long customerId, long tenantId) {
        // customerId/tenantId are numeric ids, so no URL-encoding is needed.
        String url = baseUrl + API_PATH + objectType + "/list"
            + "?customerId=" + customerId + "&tenantId=" + tenantId;
        return HttpRequest.newBuilder(URI.create(url))
            .timeout(REQUEST_TIMEOUT)
            .header("Accept", "application/json")
            .GET()
            .build();
    }

    // ---- staging -------------------------------------------------------------

    /** Parse the JSON map (via DuckDB) and append active rows to the stage table. */
    private static void stage(Connection conn, Sync s, String json) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(s.stageSql())) {
            ps.setString(1, json);
            ps.executeUpdate();
        }
    }

    // INVARIANT: this set of pairs defines the capture scope. Because absence from
    // the snapshot is a delete signal and the target tables are not tenant-scoped,
    // removing a pair here will delete that pair's still-live rows on the next pass.
    // Only remove a pair when its captured rows are genuinely meant to be purged.
    private static List<long[]> readCustomerTenantPairs(Statement st) throws SQLException {
        List<long[]> pairs = new ArrayList<>();
        try (ResultSet rs = st.executeQuery(
            "SELECT customer_id, tenant_id FROM ollylake.main.customer_tenant_reference "
                + "ORDER BY customer_id, tenant_id")) {
            while (rs.next()) {
                pairs.add(new long[] {rs.getLong(1), rs.getLong(2)});
            }
        }
        return pairs;
    }

    // ---- small helpers -------------------------------------------------------

    private static String columns(List<Col> cols) {
        return join(cols, Col::column, ", ");
    }

    private static <T> String join(List<T> items, Function<T, String> render, String sep) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                sb.append(sep);
            }
            sb.append(render.apply(items.get(i)));
        }
        return sb.toString();
    }

    private static String trimSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    /** Split a multi-statement SQL script into statements (see {@link SqlScripts}). */
    static List<String> splitStatements(String sql) {
        return SqlScripts.splitStatements(sql);
    }

    private SccalReferenceSync() {
    }
}
