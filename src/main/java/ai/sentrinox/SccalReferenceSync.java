package ai.sentrinox;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

    private static final Logger log = LoggerFactory.getLogger(SccalReferenceSync.class);

    static final String API_PATH = "/internal/sccal/api/v1/";

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
            return stageName("cdc_stg_");
        }

        private String stageName(String prefix) {
            return prefix + table.replaceAll("[^A-Za-z0-9]", "_");
        }

        /** {@code t.k1 = s.k1 AND t.k2 = s.k2} for the staging-vs-target key match. */
        private String keyEq() {
            return join(keyCols, c -> "t." + c.column() + " = s." + c.column(), " AND ");
        }

        /** {@code col1 TYPE1, col2 TYPE2} column DDL for a staging table. */
        private String columnDefs() {
            return join(allCols(), c -> c.column() + " " + c.sqlType(), ", ");
        }

        /** {@code d1 = s.d1, d2 = s.d2} SET clause over the data columns. */
        private String setClause() {
            return join(dataCols, c -> c.column() + " = s." + c.column(), ", ");
        }

        private String createTempSql() {
            return "CREATE TEMP TABLE " + stage() + " (" + columnDefs() + ")";
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
            return "UPDATE " + table + " t SET " + setClause() + " FROM " + stage() + " s WHERE "
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

        // ---- change-stream variants ------------------------------------------
        // Fed from a /changes page (entity-change stream) instead of a snapshot.
        // Deletes are explicit tombstones (action = 'DELETE'), never inferred
        // from absence, so none of the snapshot path's empty-stage guarding is
        // needed here.

        /** Temp staging table for /changes entries, unique per target table. */
        String changeStage() {
            return stageName("cdc_chg_");
        }

        String createChangeStageSql() {
            return "CREATE OR REPLACE TEMP TABLE " + changeStage() + " (" + columnDefs()
                + ", __action VARCHAR, __change_id BIGINT)";
        }

        /**
         * Parameterised: bind one raw /changes response body as parameter 1.
         * Stages the page's entries of {@code entityType}, pulling the columns
         * out of each entry's {@code sccal} payload — aliased {@code e}, so
         * {@link Col#expr()} and {@link #filter} apply unchanged.
         *
         * <p>Two guards align this with the snapshot path's delete semantics:
         * a non-DELETE action whose payload says {@code isDeleted:true} (a
         * soft-delete modelled as an update) is normalised to a tombstone, and
         * tombstones bypass {@link #filter} — a minimal DELETE payload may omit
         * the filter field, and a NULL predicate would silently drop the delete.
         * Deleting by key is safe for a sibling table the entity doesn't live
         * in: the key simply matches nothing there.
         */
        String stageChangesSql(String entityType) {
            String sql = "INSERT INTO " + changeStage() + " (" + columns(allCols())
                + ", __action, __change_id) SELECT " + join(allCols(), Col::expr, ", ")
                + ", __action, __change_id FROM ("
                + "SELECT entry->'sccal' AS e,"
                + " CASE WHEN coalesce((entry->'sccal'->>'isDeleted')::BOOLEAN, false)"
                + " THEN 'DELETE' ELSE entry->>'action' END AS __action,"
                + " (entry->>'id')::BIGINT AS __change_id"
                + " FROM (SELECT unnest(json_extract(?::JSON, '$.entries[*]')) AS entry)"
                + " WHERE entry->>'entityType' = '" + entityType + "')";
            return filter == null ? sql : sql + " WHERE (__action = 'DELETE' OR " + filter + ")";
        }

        /**
         * Collapse to the LAST event per key (highest change id) — a page can
         * carry several events for one key (e.g. CREATE then DELETE) and only
         * the final state may be applied.
         */
        String changeDedupeSql() {
            return "CREATE OR REPLACE TEMP TABLE " + changeStage() + " AS SELECT * FROM " + changeStage()
                + " QUALIFY row_number() OVER (PARTITION BY " + columns(keyCols)
                + " ORDER BY __change_id DESC) = 1";
        }

        String changeDeleteSql() {
            return "DELETE FROM " + table + " t WHERE EXISTS (SELECT 1 FROM " + changeStage()
                + " s WHERE " + keyEq() + " AND s.__action = 'DELETE')";
        }

        String changeUpdateSql() {
            return "UPDATE " + table + " t SET " + setClause() + " FROM " + changeStage() + " s WHERE "
                + keyEq() + " AND s.__action <> 'DELETE' AND " + notNull(dataCols)
                + " AND (" + changedPredicate() + ")";
        }

        String changeInsertSql() {
            String cols = columns(allCols());
            return "INSERT INTO " + table + " (" + cols + ") SELECT " + cols + " FROM " + changeStage()
                + " s WHERE s.__action <> 'DELETE' AND " + notNull(allCols()) + " AND NOT EXISTS "
                + "(SELECT 1 FROM " + table + " t WHERE " + keyEq() + ")";
        }

        /** {@code s.c1 IS NOT NULL AND s.c2 IS NOT NULL} over the given columns. */
        private String notNull(List<Col> cols) {
            return join(cols, c -> "s." + c.column() + " IS NOT NULL", " AND ");
        }

        /**
         * Apply one staged /changes page (dedupe, then DELETE / UPDATE / INSERT);
         * returns {inserted, updated, deleted}. Replays are idempotent: a re-seen
         * CREATE hits NOT EXISTS, a re-seen UPDATE fails the changed predicate,
         * a re-seen DELETE matches nothing.
         *
         * <p>Non-tombstone rows with a NULL column are counted, warned about and
         * skipped (the NOT NULL guards in the insert/update SQL): applying one
         * would violate the target's NOT NULL constraints, abort the whole
         * transaction with the cursor unmoved, and refail on the same page every
         * cycle — a single poison entry must not freeze the stream.
         */
        int[] applyChanges(Statement st) throws SQLException {
            st.execute(changeDedupeSql());
            int skipped = countUnappliable(st);
            if (skipped > 0) {
                log.warn("change stream: {} {} event(s) skipped — payload missing a required"
                    + " column (entry preserved in the audit log)", skipped, displayName());
            }
            return applyDml(st, false, changeDeleteSql(), changeUpdateSql(), changeInsertSql());
        }

        /** Staged non-tombstone rows with a NULL column: not insertable/updatable. */
        private int countUnappliable(Statement st) throws SQLException {
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM " + changeStage()
                + " s WHERE s.__action <> 'DELETE' AND NOT (" + notNull(allCols()) + ")")) {
                rs.next();
                return rs.getInt(1);
            }
        }

        /** Apply DELETE / UPDATE / INSERT; returns {inserted, updated, deleted}. */
        private int[] merge(Statement st) throws SQLException {
            // An empty staging table is ambiguous: it can mean "this type genuinely
            // has no rows" but also "the API returned a blank/degenerate-but-200
            // body" (coalesced to "{}") or "every pair failed". Treating that as a
            // delete would wipe the whole target table on a transient hiccup, so
            // skip DELETE when staging is empty and leave the existing rows intact.
            return applyDml(st, stageIsEmpty(st), deleteSql(), updateSql(), insertSql());
        }

        /**
         * The shared apply sequence of both capture paths; returns
         * {inserted, updated, deleted} — the order every totals consumer assumes.
         * UPDATE is skipped for edge tables (no data columns to change).
         */
        private int[] applyDml(Statement st, boolean skipDelete, String delete, String update,
                               String insert) throws SQLException {
            int deleted = skipDelete ? 0 : st.executeUpdate(delete);
            int updated = dataCols.isEmpty() ? 0 : st.executeUpdate(update);
            int inserted = st.executeUpdate(insert);
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

    // The reference tables and the object type each is captured from.
    // RULE feeds two tables, split by the ruleType enum: 0 = TenantProvider /
    // 1 = WorkspaceProvider are LLM/provider access rules, 2 = TenantMcp /
    // 3 = WorkspaceMcp are MCP access rules (tenant vs. workspace scope).
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
            List.of(new Col("name", "name", "VARCHAR")), "(e->>'ruleType')::INTEGER IN (0, 1)"),
        new Sync("rule", "ollylake.main.mcp_access_rule",
            List.of(new Col("mcp_access_rule_id", "ruleId", "BIGINT")),
            List.of(new Col("name", "name", "VARCHAR")), "(e->>'ruleType')::INTEGER IN (2, 3)")
    );

    /**
     * Object types now fed from the entity-change stream ({@code /changes})
     * instead of the per-cycle full-snapshot diff: snapshot-API objectType →
     * the {@code /changes} entityType that feeds it (the stream emits enum
     * names, the snapshot API path segments). The SINGLE source of truth for
     * the stream/snapshot split — {@link #STREAM_SYNCS}, {@link #SNAPSHOT_SYNCS}
     * and {@link ChangeStreamSync#SYNCS_BY_ENTITY_TYPE} all derive from it, so
     * promoting a type to the stream is one entry here. Grows as SCCAL wires
     * more capture types (USER + ACTIVATION today; ACTIVATION feeds no
     * reference table). The snapshot machinery still covers these types on
     * bootstrap and on a 410 Gone resync — see {@link ChangeStreamSync}.
     */
    static final Map<String, String> STREAM_ENTITY_TYPES = Map.of("user", "USER");

    /** Stream-fed syncs (bootstrap / resync scope). Declared after SYNCS. */
    static final List<Sync> STREAM_SYNCS = SYNCS.stream()
        .filter(s -> STREAM_ENTITY_TYPES.containsKey(s.objectType())).toList();

    /** Syncs still captured by snapshot diff every cycle. */
    static final List<Sync> SNAPSHOT_SYNCS = SYNCS.stream()
        .filter(s -> !STREAM_ENTITY_TYPES.containsKey(s.objectType())).toList();

    /** objectType -> the sync(s) it feeds (RULE feeds two), in declared order. */
    private static Map<String, List<Sync>> groupByType(List<Sync> syncs) {
        Map<String, List<Sync>> byType = new LinkedHashMap<>();
        for (Sync s : syncs) {
            byType.computeIfAbsent(s.objectType(), k -> new ArrayList<>()).add(s);
        }
        return byType;
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load().getConfig("analytics_cdc");
        String baseUrl = trimSlash(config.getString("sccal_base_url"));
        Duration pollInterval = config.getDuration("poll_interval");

        log.info("SCCAL reference sync — API: {}", baseUrl);

        HttpClient http = SccalHttp.newClient();

        // Bootstrap once (extensions + S3 secret + ATTACH); the warm connection is
        // reused across every cycle.
        try (Connection conn = SqlScripts.bootstrap(config);
             Statement st = conn.createStatement()) {

            if (isOneShot(pollInterval)) {
                runOnce(conn, st, http, baseUrl);          // one-shot
                log.info("SCCAL reference sync complete.");
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
        log.info("Polling every {}s (Ctrl-C to stop).", interval.toSeconds());
        while (true) {
            log.info("--- sync cycle ---");
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
            log.error("sync cycle failed — retrying next interval", e);
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

    /**
     * One full capture pass, two phases:
     * <ol>
     *   <li><b>change stream</b> — stream-fed types via {@code /cursors}
     *       discovery + per-tenant {@code /changes} deltas (usually a single
     *       cheap idle call);</li>
     *   <li><b>snapshot diff</b> — every other type, fetched in full and
     *       diffed, exactly as before.</li>
     * </ol>
     */
    static void runOnce(Connection conn, Statement st, HttpClient http,
                        String baseUrl) throws SQLException {
        List<long[]> pairs = readCustomerTenantPairs(st);
        if (pairs.isEmpty()) {
            log.info("No (customerId, tenantId) pairs in customer_tenant_reference"
                + " — nothing to capture.");
            return;
        }
        // The phases are independent (disjoint tables, separate transactions):
        // a stream failure — endpoint down, pruned offset, missing audit table —
        // must never block snapshot capture for the other dimension tables. But
        // it must still SURFACE after the snapshot phase: the stream-fed tables
        // have no snapshot fallback in steady state, so a swallowed persistent
        // failure would leave them silently stale — and let a one-shot run
        // (cron/CI) exit 0 with monitoring none the wiser.
        Exception streamFailure = null;
        try {
            ChangeStreamSync.run(conn, st, http, baseUrl, pairs);
        } catch (SQLException | RuntimeException e) {
            log.warn("change stream phase failed — continuing with the snapshot phase", e);
            resetConnection(conn);
            streamFailure = e;
        }
        stageAll(conn, st, http, baseUrl, pairs, SNAPSHOT_SYNCS);
        applyCapture(conn, st, SNAPSHOT_SYNCS);
        if (streamFailure != null) {
            throw new IllegalStateException(
                "change stream phase failed (snapshot phase completed)", streamFailure);
        }
    }

    /**
     * Fetch the given syncs' object types and stage their rows; globals once,
     * tenant types per pair. Returns the total number of rows staged — zero
     * means every response was empty or degenerate, which the resync path uses
     * to withhold cursor seeding.
     */
    static long stageAll(Connection conn, Statement st, HttpClient http,
                         String baseUrl, List<long[]> pairs, List<Sync> syncs) throws SQLException {
        Map<String, List<Sync>> byType = groupByType(syncs);
        List<String> globalTypes = byType.keySet().stream().filter(GLOBAL_TYPES::contains).toList();
        List<String> tenantTypes = byType.keySet().stream().filter(t -> !GLOBAL_TYPES.contains(t)).toList();

        for (Sync s : syncs) {
            st.execute("DROP TABLE IF EXISTS " + s.stage());
            st.execute(s.createTempSql());
        }

        long staged = 0;
        // Global types ignore tenant scoping — fetch once (params still required).
        if (!globalTypes.isEmpty()) {
            long[] any = pairs.get(0);
            staged += stageResponses(conn, byType, fetchAll(http, baseUrl, globalTypes, any[0], any[1]));
        }
        // Tenant-scoped types — fetched concurrently for each pair.
        if (!tenantTypes.isEmpty()) {
            for (long[] pair : pairs) {
                staged += stageResponses(conn, byType, fetchAll(http, baseUrl, tenantTypes, pair[0], pair[1]));
            }
        }
        return staged;
    }

    /** Parse each JSON map (via DuckDB) and append its active rows to the fed stage tables. */
    private static long stageResponses(Connection conn, Map<String, List<Sync>> byType,
                                       Map<String, String> responses) throws SQLException {
        long staged = 0;
        for (Map.Entry<String, String> e : responses.entrySet()) {
            for (Sync s : byType.get(e.getKey())) {
                staged += DuckJson.executeWithJson(conn, s.stageSql(), e.getValue());
            }
        }
        return staged;
    }

    /** De-dupe staging, then merge every table inside one transaction (atomic snapshot). */
    private static void applyCapture(Connection conn, Statement st, List<Sync> syncs) throws SQLException {
        printCaptureHeader();
        try {
            SqlScripts.inTransaction(conn, () -> {
                int[] totals = mergeAll(st, syncs);
                printCaptureTotals(totals);
                // Nothing changed → roll back so we don't write an empty DuckLake snapshot.
                return totals[0] + totals[1] + totals[2] > 0;
            });
        } finally {
            dropStages(st, syncs);
        }
    }

    /**
     * De-dupe each stage then merge it; prints one row per table and returns
     * {ins, upd, del} totals. No transaction management — the caller owns the
     * transaction (so cursor-state writes can join it).
     */
    static int[] mergeAll(Statement st, List<Sync> syncs) throws SQLException {
        for (Sync s : syncs) {
            st.execute(s.dedupeSql());
        }
        int[] totals = new int[3];
        for (Sync s : syncs) {
            int[] c = s.merge(st);
            addCounts(totals, c);
            log.info(String.format("%-28s %7d %7d %7d", s.displayName(), c[0], c[1], c[2]));
        }
        return totals;
    }

    /** Accumulate {ins, upd, del} deltas into the running totals, in place. */
    static void addCounts(int[] totals, int[] delta) {
        for (int i = 0; i < delta.length; i++) {
            totals[i] += delta[i];
        }
    }

    static void printCaptureHeader() {
        log.info(String.format("%-28s %7s %7s %7s", "table", "ins", "upd", "del"));
        log.info("-".repeat(52));
    }

    static void printCaptureTotals(int[] totals) {
        log.info("-".repeat(52));
        log.info(String.format("%-28s %7d %7d %7d", "TOTAL", totals[0], totals[1], totals[2]));
    }

    static void dropStages(Statement st, List<Sync> syncs) throws SQLException {
        for (Sync s : syncs) {
            st.execute("DROP TABLE IF EXISTS " + s.stage());
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
            HttpResponse<String> resp = SccalHttp.join(e.getValue(), "fetch failed for "
                + e.getKey() + " (customerId=" + customerId + ", tenantId=" + tenantId + ")");
            bodies.put(e.getKey(), SccalHttp.requireOk(resp));
        }
        return bodies;
    }

    private static HttpRequest request(String baseUrl, String objectType,
                                       long customerId, long tenantId) {
        // customerId/tenantId are numeric ids, so no URL-encoding is needed.
        return SccalHttp.jsonGet(baseUrl + API_PATH + objectType + "/list"
            + "?customerId=" + customerId + "&tenantId=" + tenantId);
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

    private SccalReferenceSync() {
    }
}
