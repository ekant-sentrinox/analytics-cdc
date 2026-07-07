package ai.sentrinox;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Change-data-capture job for the analytics reference (dimension) tables.
 *
 * <p>Captures the reference tables purely from the SCCAL entity-change stream —
 * the two polling endpoints ({@code GET /internal/sccal/api/v1/cursors} and
 * {@code GET /internal/sccal/api/v1/changes}). The {@code (customerId,
 * tenantId)} pairs are discovered from the cursor registry itself
 * ({@code /cursors}), so a new tenant is captured automatically the first time
 * it appears there. There is no full-snapshot ({@code /list}) path: every
 * INSERT / UPDATE / DELETE comes from an explicit change entry, so deletes are
 * tombstones (never inferred from absence). See {@link ChangeStreamSync} for
 * the two-level poll, first-contact bootstrap (replay from the start of a
 * pair's stream) and 410-Gone (fast-forward past a pruned offset).
 *
 * <p>JSON is parsed by DuckDB itself (the raw response is bound as a {@code JSON}
 * parameter and shredded with {@code json_extract(...)}), so the job needs no
 * JSON library beyond the DuckDB driver already on the classpath.
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
     * One column of a reference table and how to pull it out of an entity.
     *
     * <p>A column may map to more than one JSON field. The {@code /changes}
     * stream carries two payload conventions in the same feed — a generic shape
     * ({@code id}, {@code name}, {@code email}) and the real Sentrinox shape
     * ({@code userId}, {@code userName}, ...) — so the extraction coalesces the
     * alternatives (declared most-specific first) and takes the first present.
     */
    private record Col(String column, List<String> jsonFields, String sqlType) {
        Col(String column, String jsonField, String sqlType) {
            this(column, List.of(jsonField), sqlType);
        }

        /**
         * Raw text extraction from entity alias {@code e}, aliased to the target
         * column — no cast. Materialized in an inner projection so the typed
         * {@link #castExpr()} in the outer projection references a plain column
         * rather than {@code e}: mixing {@code (e->>'f')::TYPE} casts with the
         * delete-normalising CASE (which also reads {@code e}) in one projection
         * trips a DuckDB (1.4.5) planner conversion bug — the same one the audit
         * shredder sidesteps.
         */
        String rawExpr() {
            String extract = jsonFields.size() == 1
                ? "e->>'" + jsonFields.get(0) + "'"
                : "coalesce(" + join(jsonFields, f -> "e->>'" + f + "'", ", ") + ")";
            return extract + " AS " + column;
        }

        /** Cast the materialized raw column (see {@link #rawExpr()}) to its SQL type. */
        String castExpr() {
            return column + "::" + sqlType + " AS " + column;
        }
    }

    /**
     * A reference table fed from one SCCAL object type, with the SQL needed to
     * stage and capture it from the change stream.
     *
     * <p>Every target table also carries a {@code customer_id}. It is not in the
     * entity payload — it comes from the poll context (the customer whose stream
     * is being pulled) — so it is staged as a literal and is part of the CDC key,
     * scoping every match to the owning customer (the tables are also
     * {@code PARTITIONED BY (customer_id)}).
     *
     * @param objectType SCCAL object type ({@code user}, {@code workspace},
     *                   {@code group}); the {@code /changes} entityType it maps
     *                   to is declared in {@link #STREAM_ENTITY_TYPES}
     * @param table      target table, already schema-qualified and quoted
     * @param keyCols    identity columns (the per-customer CDC key)
     * @param dataCols   non-key attribute columns (e.g. name)
     */
    record Sync(String objectType, String table, List<Col> keyCols,
                List<Col> dataCols) {

        private List<Col> allCols() {
            List<Col> all = new ArrayList<>(keyCols);
            all.addAll(dataCols);
            return all;
        }

        private String stageName(String prefix) {
            return prefix + table.replaceAll("[^A-Za-z0-9]", "_");
        }

        /**
         * {@code t.customer_id = s.customer_id AND t.k1 = s.k1 ...} — the
         * staging-vs-target key match, always scoped by customer_id so a row is
         * only ever matched within its owning customer.
         */
        private String keyEq() {
            return "t.customer_id = s.customer_id AND "
                + join(keyCols, c -> "t." + c.column() + " = s." + c.column(), " AND ");
        }

        /** {@code col1 TYPE1, col2 TYPE2} column DDL for a staging table. */
        private String columnDefs() {
            return join(allCols(), c -> c.column() + " " + c.sqlType(), ", ");
        }

        /** {@code d1 = s.d1, d2 = s.d2} SET clause over the data columns. */
        private String setClause() {
            return join(dataCols, c -> c.column() + " = s." + c.column(), ", ");
        }

        private String changedPredicate() {
            return join(dataCols, c -> "t." + c.column() + " IS DISTINCT FROM s." + c.column(), " OR ");
        }

        // ---- change-stream staging + merge -----------------------------------
        // Fed from a /changes page (entity-change stream). Deletes are explicit
        // tombstones (action = 'DELETE'), never inferred from absence.

        /** Temp staging table for /changes entries, unique per target table. */
        String changeStage() {
            return stageName("cdc_chg_");
        }

        String createChangeStageSql() {
            return "CREATE OR REPLACE TEMP TABLE " + changeStage() + " (customer_id BIGINT, "
                + columnDefs() + ", __action VARCHAR, __change_id BIGINT)";
        }

        /**
         * Parameterised: bind one raw /changes response body as parameter 1.
         * Stages the page's entries of {@code entityType}, pulling the columns
         * out of each entry's {@code sccal} payload — aliased {@code e} — and
         * stamping {@code customerId} (the poll context, not in the payload) as a
         * literal on every row.
         *
         * <p>Delete semantics: a non-DELETE action whose payload says
         * {@code isDeleted:true} (the real shape) or {@code op:"delete"} (the
         * generic shape) — a soft-delete modelled as an update — is normalised
         * to a tombstone, so it removes the row rather than being applied as an
         * update.
         *
         * <p>Two projections over {@code e}: the inner one materializes every
         * field access ({@code isDeleted}, {@code op} and each column) as a raw
         * text column ({@link Col#rawExpr()}); the outer one applies the typed
         * casts ({@link Col#castExpr()}) and the delete-normalising CASE against
         * those plain columns, never {@code e}. Mixing the {@code ::TYPE} casts
         * and the CASE in one projection over {@code e} trips a DuckDB (1.4.5)
         * planner conversion bug — the same one the audit shredder sidesteps.
         */
        String stageChangesSql(String entityType, long customerId) {
            return "INSERT INTO " + changeStage() + " (customer_id, " + columns(allCols())
                + ", __action, __change_id) SELECT " + customerId + ", "
                + join(allCols(), Col::castExpr, ", ")
                + ", CASE WHEN coalesce(__is_deleted::BOOLEAN, false) OR __op = 'delete'"
                + " THEN 'DELETE' ELSE action END AS __action, __change_id FROM ("
                + "SELECT " + join(allCols(), Col::rawExpr, ", ")
                + ", e->>'isDeleted' AS __is_deleted, e->>'op' AS __op,"
                + " action, __change_id FROM ("
                + "SELECT entry->'sccal' AS e, entry->>'action' AS action,"
                + " (entry->>'id')::BIGINT AS __change_id"
                + " FROM (SELECT unnest(json_extract(?::JSON, '$.entries[*]')) AS entry)"
                + " WHERE entry->>'entityType' = '" + entityType + "'))";
        }

        /**
         * Collapse to the LAST event per key (highest change id) — a page can
         * carry several events for one key (e.g. CREATE then DELETE) and only
         * the final state may be applied.
         */
        String changeDedupeSql() {
            return "CREATE OR REPLACE TEMP TABLE " + changeStage() + " AS SELECT * FROM " + changeStage()
                + " QUALIFY row_number() OVER (PARTITION BY customer_id, " + columns(keyCols)
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
            String cols = "customer_id, " + columns(allCols());
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
            return applyDml(st, changeDeleteSql(), changeUpdateSql(), changeInsertSql());
        }

        /** Staged non-tombstone rows with a NULL column: not insertable/updatable. */
        private int countUnappliable(Statement st) throws SQLException {
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM " + changeStage()
                + " s WHERE s.__action <> 'DELETE' AND NOT (" + notNull(allCols()) + ")")) {
                rs.next();
                return rs.getInt(1);
            }
        }

        /**
         * Apply DELETE / UPDATE / INSERT; returns {inserted, updated, deleted} —
         * the order every totals consumer assumes. UPDATE is skipped for edge
         * tables (no data columns to change).
         */
        private int[] applyDml(Statement st, String delete, String update, String insert)
                throws SQLException {
            int deleted = st.executeUpdate(delete);
            int updated = dataCols.isEmpty() ? 0 : st.executeUpdate(update);
            int inserted = st.executeUpdate(insert);
            return new int[] {inserted, updated, deleted};
        }

        private String displayName() {
            int dot = table.lastIndexOf('.');
            return (dot < 0 ? table : table.substring(dot + 1)).replace("\"", "");
        }
    }

    // The reference tables and the object type each is captured from. The
    // /changes stream only ever emits USER, WORKSPACE and GROUP entities, so
    // those are the only object types captured; every other V6 table has no
    // corresponding stream entity and is left empty.
    //
    // Each column coalesces the two payload conventions the stream mixes (see
    // Col): the real Sentrinox shape ({userId, userName, isDeleted}) declared
    // first, then the generic shape ({id, name, email}). Whichever the entry
    // carries, the row is captured.
    static final List<Sync> SYNCS = List.of(
        new Sync("workspace", "ollylake.main.workspace",
            List.of(new Col("workspace_id", List.of("workspaceId", "id"), "BIGINT")),
            List.of(new Col("name", List.of("workspaceName", "name"), "VARCHAR"))),
        new Sync("user", "ollylake.main.\"user\"",
            List.of(new Col("user_id", List.of("userId", "id"), "BIGINT")),
            List.of(new Col("name", List.of("userName", "email"), "VARCHAR"))),
        new Sync("group", "ollylake.main.\"group\"",
            List.of(new Col("group_id", List.of("groupId", "id"), "BIGINT")),
            List.of(new Col("name", List.of("groupName", "name"), "VARCHAR")))
    );

    /**
     * SCCAL object type → the {@code /changes} entityType that feeds it (the
     * stream emits enum names, the snapshot API used lowercase path segments).
     * Every reference table is fed from the change stream, so every object type
     * in {@link #SYNCS} appears here. The SINGLE source of truth for the
     * type→entityType mapping — {@link ChangeStreamSync#SYNCS_BY_ENTITY_TYPE}
     * derives from it. ACTIVATION entries also flow through the stream (audited)
     * but feed no reference table, so they have no entry here.
     */
    static final Map<String, String> STREAM_ENTITY_TYPES = Map.of(
        "workspace", "WORKSPACE",
        "user", "USER",
        "group", "GROUP");

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load().getConfig("analytics_cdc");
        String baseUrl = trimSlash(config.getString("sccal_base_url"));
        Duration pollInterval = config.getDuration("poll_interval");

        log.info("SCCAL reference sync — API: {}", baseUrl);

        HttpClient http = SccalHttp.newClient();

        if (isOneShot(pollInterval)) {
            // One-shot: bootstrap once (extensions + S3 secret + ATTACH) and run.
            try (Connection conn = SqlScripts.bootstrap(config);
                 Statement st = conn.createStatement()) {
                runOnce(conn, st, http, baseUrl);
                log.info("SCCAL reference sync complete.");
            }
        } else {
            // Polling: poll() owns the connection so it can rebuild it after a
            // failure that leaves it unusable (see the loop below).
            poll(config, http, baseUrl, pollInterval);
        }
    }

    /** A zero or negative {@code poll_interval} means run once and exit (the default). */
    static boolean isOneShot(Duration pollInterval) {
        return pollInterval.isZero() || pollInterval.isNegative();
    }

    /**
     * Repeatedly capture on a fixed interval, surviving per-cycle failures.
     *
     * <p>Owns the DuckDB connection so it can rebuild it: a failed cycle can
     * abort the DuckLake catalog transaction on the shared Postgres connection
     * (which an in-place rollback cannot always clear) and can close the JDBC
     * statement. Before each cycle the connection + statement are (re)bootstrapped
     * if unusable — a fresh ATTACH gives a fresh catalog connection — so a single
     * transient catalog error can no longer wedge the poller permanently.
     */
    private static void poll(Config config, HttpClient http, String baseUrl,
                             Duration interval) throws InterruptedException {
        log.info("Polling every {}s (Ctrl-C to stop).", interval.toSeconds());
        Connection conn = null;
        Statement st = null;
        try {
            while (true) {
                log.info("--- sync cycle ---");
                if (!usable(conn, st)) {
                    try {
                        closeQuietly(conn);        // closes st with it, if any
                        conn = SqlScripts.bootstrap(config);
                        st = conn.createStatement();
                    } catch (Exception e) {
                        log.error("could not (re)establish the DuckDB connection"
                            + " — retrying next interval", e);
                        closeQuietly(conn);
                        conn = null;
                        st = null;
                        Thread.sleep(interval.toMillis());
                        continue;
                    }
                }
                pollCycle(conn, st, http, baseUrl);
                Thread.sleep(interval.toMillis());
            }
        } finally {
            closeQuietly(conn);
        }
    }

    /** True when the connection and statement are both open and reusable. */
    private static boolean usable(Connection conn, Statement st) {
        try {
            return conn != null && !conn.isClosed() && st != null && !st.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    /** Close a connection (and its statements) ignoring any error. */
    private static void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // Already closed or unusable — nothing more to do.
            }
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
            // The failure may have left the connection mid-transaction with
            // autoCommit still false. Reset it so this stale state can't leak
            // into the next cycle's writes.
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
     * One full capture pass: drive the change stream — {@code /cursors}
     * discovery (which also defines the capture scope) + per-tenant
     * {@code /changes} deltas (usually a single cheap idle call), with a
     * first-contact bootstrap that replays a newly seen pair's stream from the
     * start. See {@link ChangeStreamSync}.
     */
    static void runOnce(Connection conn, Statement st, HttpClient http,
                        String baseUrl) throws SQLException {
        ChangeStreamSync.run(conn, st, http, baseUrl);
    }

    // ---- small helpers -------------------------------------------------------

    private static String columns(List<Col> cols) {
        return join(cols, Col::column, ", ");
    }

    private static <T> String join(List<T> items, Function<T, String> render, String sep) {
        return items.stream().map(render).collect(Collectors.joining(sep));
    }

    private static String trimSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private SccalReferenceSync() {
    }
}
