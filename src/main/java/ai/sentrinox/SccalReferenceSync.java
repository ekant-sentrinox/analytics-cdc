package ai.sentrinox;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Change-data-capture job for the analytics reference (dimension) tables.
 * Two capture paths run per cycle:
 * <ul>
 *   <li><b>tenant tier</b> — the SCCAL entity-change stream (deletes are
 *       explicit tombstones); the capture mechanics (two-level poll,
 *       first-contact bootstrap, 410 recovery) live in
 *       {@link ChangeStreamSync};</li>
 *   <li><b>global tier</b> — the catalogue dimensions (providers, models,
 *       MCP servers/tools) plus tenant, which are deliberately not in the
 *       change stream and sync by snapshot + {@code updatedSince} incremental
 *       {@code /list} pull ({@link CatalogueListSync}).</li>
 * </ul>
 * This class owns the process lifecycle and the config-declared table
 * definitions ({@link Sync}).
 *
 * <p>JSON is parsed by DuckDB itself (bodies bound as {@code JSON} parameters
 * and shredded with {@code json_extract}), so the job needs no JSON library.
 *
 * <p>A zero {@code analytics_cdc.poll_interval} runs one capture and exits; a
 * positive interval polls forever, reusing the warm DuckDB connection and
 * surviving per-cycle failures.
 *
 * <p>Run with: {@code java -cp analytics-cdc.jar ai.sentrinox.SccalReferenceSync}
 */
public final class SccalReferenceSync {

    private static final Logger log = LoggerFactory.getLogger(SccalReferenceSync.class);

    static final String API_PATH = "/internal/sccal/api/v1/";

    /**
     * Per-page temp table of shredded /changes entries — created and loaded
     * once per page by {@link ChangeStreamSync} (one JSON parse), then read by
     * the audit insert and every sync's staging instead of each re-parsing the
     * raw body. Keeps the raw {@code entry} column so config
     * {@code entry_filter} predicates written against it still apply.
     */
    static final String ENTRIES_STAGE = "cdc_entries";

    /**
     * One column of a reference table and the JSON field(s) it is pulled from.
     * The {@code /changes} stream mixes two payload conventions (the real
     * Sentrinox shape — {@code userId}, {@code userName} — and a generic
     * {@code id}/{@code name}/{@code email} shape), so extraction coalesces
     * the declared fields, most-specific first. Shared with the catalogue
     * /list capture ({@link CatalogueListSync}), whose payloads it extracts
     * the same way.
     */
    record Col(String column, List<String> jsonFields, String sqlType) {

        /**
         * Raw text extraction from entity alias {@code e}, aliased to the
         * target column, no cast — the cast happens in a separate outer
         * projection ({@link #castExpr()}); see {@link Sync#stageChangesSql}
         * for the DuckDB planner bug that split sidesteps.
         */
        String rawExpr() {
            String extract = jsonFields.size() == 1
                ? "e->>'" + jsonFields.get(0) + "'"
                : "coalesce(" + join(jsonFields, f -> "e->>'" + f + "'", ", ") + ")";
            return extract + " AS " + column;
        }

        /**
         * TRY_CAST the materialized raw column (see {@link #rawExpr()}) to its
         * SQL type. TRY_CAST, not a hard cast: a malformed value (e.g. a
         * non-numeric id) becomes NULL and flows into the poison-entry skip
         * path — a hard cast would abort the transaction with the cursor
         * unmoved and refail on the same page every cycle.
         */
        String castExpr() {
            return "TRY_CAST(" + column + " AS " + sqlType + ") AS " + column;
        }
    }

    /** One merge application's deltas: rows inserted / updated / soft-deleted. */
    record MergeCounts(int inserted, int updated, int deleted) {
    }

    /**
     * A reference table fed from one SCCAL {@code /changes} entityType, with
     * the SQL to stage and capture it from the change stream. Built from config
     * ({@code analytics_cdc.syncs} — see application.conf) by {@link #loadSyncs}.
     * The SQL-text fragments shared with the catalogue path live as statics on
     * this class (see the "shared SQL-text helpers" section below).
     *
     * <p>Every target table also carries a {@code customer_id}. It is not in
     * the entity payload — it comes from the poll context — so it is staged as
     * a literal and is part of the CDC key, scoping every match to the owning
     * customer.
     *
     * @param entityType     {@code /changes} entityType that feeds the table;
     *                       several syncs may share one (split by
     *                       {@code entryFilterSql})
     * @param table          target table, schema-qualified and quoted
     * @param keyCols        identity columns (the per-customer CDC key)
     * @param dataCols       non-key attribute columns; may be empty
     * @param entryFilterSql extra SQL predicate over the raw {@code entry}
     *                       JSON, ANDed with the entityType match; {@code null}
     *                       when entityType alone is enough
     */
    record Sync(String entityType, String table, List<Col> keyCols,
                List<Col> dataCols, String entryFilterSql) {

        private List<Col> allCols() {
            return SccalReferenceSync.allCols(keyCols, dataCols);
        }

        /** Staging-vs-target key match, always scoped by customer_id. */
        private String keyEq() {
            return SccalReferenceSync.keyEq(keyCols, true);
        }

        // ---- change-stream staging + merge -----------------------------------
        // Fed from a /changes page (entity-change stream). Deletes are explicit
        // tombstones (action = 'DELETE'), never inferred from absence, and are
        // applied as a soft-delete: the row is kept with is_deleted = true so
        // audit-log joins still resolve a deleted entity's name.

        /** Temp staging table for /changes entries, unique per target table. */
        String changeStage() {
            return stageName("cdc_chg_", table);
        }

        String createChangeStageSql() {
            return "CREATE OR REPLACE TEMP TABLE " + changeStage() + " (customer_id BIGINT, "
                + columnDefs(allCols()) + ", __action VARCHAR, __change_id BIGINT)";
        }

        /**
         * Stage this sync's rows of the current page from the shared
         * {@link #ENTRIES_STAGE} (already shredded — no JSON parse here):
         * columns pulled from each entry's {@code sccal} payload (alias
         * {@code e}), {@code customerId} stamped as a literal. A non-DELETE
         * action whose payload says {@code isDeleted:true} or
         * {@code op:"delete"} — a soft-delete modelled as an update — is
         * normalised to a tombstone.
         *
         * <p>Two projections on purpose: the inner one materializes every
         * field access as raw text, the outer one applies the typed casts and
         * the tombstone CASE against those plain columns, never {@code e}.
         * Mixing the casts and the CASE in one projection over {@code e} trips
         * a DuckDB planner conversion bug when an entry is missing fields
         * (verified still present on 1.5.4) — the same one the entries shred
         * sidesteps.
         */
        String stageChangesSql(long customerId) {
            return "INSERT INTO " + changeStage() + " (customer_id, " + columns(allCols())
                + ", __action, __change_id) SELECT " + customerId + ", "
                + join(allCols(), Col::castExpr, ", ")
                + ", CASE WHEN coalesce(TRY_CAST(__is_deleted AS BOOLEAN), false) OR __op = 'delete'"
                + " THEN 'DELETE' ELSE action END AS __action, __change_id FROM ("
                + "SELECT " + join(allCols(), Col::rawExpr, ", ")
                + ", e->>'isDeleted' AS __is_deleted, e->>'op' AS __op,"
                + " action, __change_id FROM ("
                + "SELECT entry->'sccal' AS e, action, change_id AS __change_id, entry"
                + " FROM " + ENTRIES_STAGE + " WHERE entity_type = '" + entityType + "'"
                + (entryFilterSql == null ? "" : " AND (" + entryFilterSql + ")") + "))";
        }

        /**
         * Collapse to the last event per key — a page can carry several events
         * for one key (CREATE then DELETE) and only the final state may apply.
         */
        String changeDedupeSql() {
            return "CREATE OR REPLACE TEMP TABLE " + changeStage() + " AS SELECT * FROM " + changeStage()
                + " QUALIFY row_number() OVER (PARTITION BY customer_id, " + columns(keyCols)
                + " ORDER BY __change_id DESC) = 1";
        }

        /**
         * Soft-delete: flip {@code is_deleted} on the live rows a tombstone
         * targets and keep them, so audit-log joins still resolve a deleted
         * entity's name. The {@code t.is_deleted = false} guard makes a
         * replayed tombstone a no-op and counts only real live→deleted
         * transitions. A tombstone for a key not present is dropped — there is
         * nothing to flag (the entity was never captured live).
         */
        String changeDeleteSql() {
            return "UPDATE " + table + " t SET is_deleted = true FROM " + changeStage()
                + " s WHERE " + keyEq() + " AND s.__action = 'DELETE' AND t.is_deleted = false";
        }

        /**
         * Update the live attributes of matched rows and, when a tombstone had
         * soft-deleted the row, revive it ({@code is_deleted = false}). Always
         * valid: the {@code notNull} guard is dropped for an edge table (no
         * data columns), leaving a pure revive.
         */
        String changeUpdateSql() {
            String notNullGuard = dataCols.isEmpty() ? "" : " AND " + notNull(dataCols);
            return "UPDATE " + table + " t SET " + setClause(dataCols) + " FROM " + changeStage()
                + " s WHERE " + keyEq() + " AND s.__action <> 'DELETE'" + notNullGuard
                + " AND (" + changedPredicate(dataCols) + ")";
        }

        String changeInsertSql() {
            String cols = "customer_id, " + columns(allCols());
            return "INSERT INTO " + table + " (" + cols + ") SELECT " + cols + " FROM " + changeStage()
                + " s WHERE s.__action <> 'DELETE' AND " + notNull(allCols()) + " AND NOT EXISTS "
                + "(SELECT 1 FROM " + table + " t WHERE " + keyEq() + ")";
        }

        /**
         * Apply one staged /changes page (dedupe, then soft-DELETE / UPDATE /
         * INSERT), where {@link MergeCounts#deleted()} counts rows flagged
         * {@code is_deleted}. Replays are idempotent: a re-seen CREATE hits NOT
         * EXISTS, a re-seen UPDATE fails the changed predicate, a re-seen
         * tombstone fails the {@code is_deleted = false} guard. The UPDATE
         * always runs — even for an edge table it maintains {@code is_deleted}
         * (revives a previously soft-deleted row).
         *
         * <p>Non-tombstone rows with a NULL column — absent OR unparseable,
         * see {@link Col#castExpr()} — are warned about and skipped (the NOT
         * NULL guards in the DML): applying one would abort the whole
         * transaction with the cursor unmoved and refail on the same page
         * every cycle — a single poison entry must not freeze the stream.
         */
        MergeCounts applyChanges(Statement st) throws SQLException {
            st.execute(changeDedupeSql());
            long skipped = SqlScripts.queryLong(st, "SELECT count(*) FROM " + changeStage()
                + " s WHERE s.__action <> 'DELETE' AND NOT (" + notNull(allCols()) + ")");
            if (skipped > 0) {
                log.warn("change stream: {} {} event(s) skipped — payload missing or"
                    + " malformed for a required column (entry preserved in the audit"
                    + " log)", skipped, displayName(table));
            }
            int deleted = st.executeUpdate(changeDeleteSql());
            int updated = st.executeUpdate(changeUpdateSql());
            int inserted = st.executeUpdate(changeInsertSql());
            return new MergeCounts(inserted, updated, deleted);
        }
    }

    // ---- shared SQL-text helpers -------------------------------------------------
    // Both capture paths (Sync above and CatalogueListSync.CatalogueSync) build
    // the same soft-delete merge DML from these fragments, so the semantics —
    // revive-on-update, IS DISTINCT FROM change detection, the poison-entry
    // NOT-NULL guards — live exactly once.

    static List<Col> allCols(List<Col> keyCols, List<Col> dataCols) {
        List<Col> all = new ArrayList<>(keyCols);
        all.addAll(dataCols);
        return all;
    }

    /** Temp staging-table name derived from the target table, unique per prefix. */
    static String stageName(String prefix, String table) {
        return prefix + table.replaceAll("[^A-Za-z0-9]", "_");
    }

    /** Staging-vs-target key match; customer-scoped when the table carries customer_id. */
    static String keyEq(List<Col> keyCols, boolean customerScoped) {
        String keys = join(keyCols, c -> "t." + c.column() + " = s." + c.column(), " AND ");
        return customerScoped ? "t.customer_id = s.customer_id AND " + keys : keys;
    }

    /** {@code col1 TYPE1, col2 TYPE2} column DDL for a staging table. */
    static String columnDefs(List<Col> cols) {
        return join(cols, c -> c.column() + " " + c.sqlType(), ", ");
    }

    /**
     * {@code d1 = s.d1, ..., is_deleted = false} SET clause: the data columns
     * plus the un-delete flag, so re-seeing an entity revives a row a prior
     * tombstone soft-deleted. Valid even with no data columns — it degrades
     * to just {@code is_deleted = false}.
     */
    static String setClause(List<Col> dataCols) {
        String unDelete = "is_deleted = false";
        if (dataCols.isEmpty()) {
            return unDelete;
        }
        return join(dataCols, c -> c.column() + " = s." + c.column(), ", ") + ", " + unDelete;
    }

    /**
     * A staged non-tombstone row differs from what is stored: some data column
     * changed, or the stored row is soft-deleted and must be revived
     * ({@code is_deleted} flipped back to false).
     */
    static String changedPredicate(List<Col> dataCols) {
        String revive = "t.is_deleted";
        if (dataCols.isEmpty()) {
            return revive;
        }
        return join(dataCols, c -> "t." + c.column() + " IS DISTINCT FROM s." + c.column(), " OR ")
            + " OR " + revive;
    }

    /** {@code s.c1 IS NOT NULL AND s.c2 IS NOT NULL} over the given columns. */
    static String notNull(List<Col> cols) {
        return join(cols, c -> "s." + c.column() + " IS NOT NULL", " AND ");
    }

    /** Bare table name for log lines: schema prefix and quoting stripped. */
    static String displayName(String table) {
        int dot = table.lastIndexOf('.');
        return (dot < 0 ? table : table.substring(dot + 1)).replace("\"", "");
    }

    /** Bare SQL identifier — column names and JSON field names. */
    static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** Uppercase enum name, as the /changes stream emits entityTypes. */
    private static final Pattern ENTITY_TYPE = Pattern.compile("[A-Z][A-Z0-9_]*");

    /** Dot-qualified table name; a part may be double-quoted (reserved words). */
    static final Pattern TABLE_NAME = Pattern.compile(
        "(?:[A-Za-z_][A-Za-z0-9_]*|\"[A-Za-z_][A-Za-z0-9_]*\")"
            + "(?:\\.(?:[A-Za-z_][A-Za-z0-9_]*|\"[A-Za-z_][A-Za-z0-9_]*\"))*");

    /** SQL type: VARCHAR, BIGINT, DECIMAL(10, 2), BIGINT[], TIMESTAMP WITH TIME ZONE, ... */
    static final Pattern SQL_TYPE = Pattern.compile(
        "[A-Za-z][A-Za-z0-9_ ]*(?:\\(\\d+(?:, ?\\d+)?\\))?(?:\\[])?");

    // The captured tables, declared in config (`analytics_cdc.syncs`) — the
    // single source of truth; adding a table is a DDL migration + config block,
    // no Java change. The config file documents the domain reasoning (emitted
    // entityTypes, the RULE fan-out, the payload conventions). Must stay below
    // the Pattern constants: static initializers run in declaration order and
    // loadSyncs validates against them.
    static final List<Sync> SYNCS = loadSyncs(ConfigFactory.load().getConfig("analytics_cdc"));

    /**
     * Build the {@link Sync} declarations from the config's {@code syncs}
     * list, failing fast on a malformed entry — a bad declaration must abort
     * startup, not silently drop a table from capture. Everything spliced into
     * generated SQL is shape-validated; {@code entry_filter} is free-form SQL
     * (trusted deploy-time input), guarded only against statement splicing.
     */
    static List<Sync> loadSyncs(Config config) {
        return loadSyncList(config, "syncs", SccalReferenceSync::parseSync, Sync::table);
    }

    private static Sync parseSync(Config c) {
        String entityType = require(ENTITY_TYPE, c.getString("entity_type"), "entity_type");
        String filter = c.hasPath("entry_filter") ? c.getString("entry_filter") : null;
        if (filter != null && (filter.isBlank() || filter.contains(";"))) {
            throw new IllegalArgumentException(
                "entry_filter must be a single non-empty predicate (no ';')");
        }
        return new Sync(entityType, c.getString("table"), parseKeyCols(c), parseDataCols(c),
            filter);
    }

    // ---- shared declaration-loading scaffolding (syncs + catalogue_syncs) --------

    /**
     * Load a config-declared sync list with the shared fail-fast checks — the
     * list must be non-empty and no target table may be declared twice — and
     * the shared per-entry error wrapping: a malformed entry aborts startup
     * naming its list and table, never silently dropping a table from capture.
     */
    static <T> List<T> loadSyncList(Config config, String path, Function<Config, T> parseEntry,
                                    Function<T, String> tableOf) {
        List<T> syncs = config.getConfigList(path).stream()
            .map(c -> parseChecked(c, path, parseEntry))
            .toList();
        if (syncs.isEmpty()) {
            throw new IllegalArgumentException(
                "analytics_cdc." + path + " is empty — no table would be captured");
        }
        if (syncs.stream().map(tableOf).distinct().count() < syncs.size()) {
            throw new IllegalArgumentException(
                "analytics_cdc." + path + " declares the same table more than once");
        }
        return syncs;
    }

    /** One entry, with the shared table-name validation and error rewrapping. */
    private static <T> T parseChecked(Config c, String path, Function<Config, T> parseEntry) {
        String table = c.getString("table");
        try {
            require(TABLE_NAME, table, "table");
            return parseEntry.apply(c);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "analytics_cdc." + path + " entry for table '" + table + "': "
                    + e.getMessage(), e);
        }
    }

    /** The identity columns of a declaration; must be non-empty. */
    static List<Col> parseKeyCols(Config c) {
        List<Col> keyCols = parseCols(c.getConfigList("key_columns"));
        if (keyCols.isEmpty()) {
            throw new IllegalArgumentException("key_columns must not be empty");
        }
        return keyCols;
    }

    /** The optional attribute columns of a declaration. */
    static List<Col> parseDataCols(Config c) {
        return c.hasPath("data_columns") ? parseCols(c.getConfigList("data_columns")) : List.of();
    }

    static List<Col> parseCols(List<? extends Config> colConfigs) {
        return colConfigs.stream().map(c -> {
            String column = require(IDENTIFIER, c.getString("column"), "column");
            List<String> fields = c.getStringList("json_fields");
            if (fields.isEmpty()) {
                throw new IllegalArgumentException(
                    "json_fields must not be empty for column " + column);
            }
            fields.forEach(f -> require(IDENTIFIER, f, "json_fields entry"));
            String type = require(SQL_TYPE, c.getString("type"), "type");
            return new Col(column, List.copyOf(fields), type);
        }).toList();
    }

    /** The value is spliced into generated SQL — reject anything outside its strict shape. */
    static String require(Pattern pattern, String value, String what) {
        if (!pattern.matcher(value).matches()) {
            throw new IllegalArgumentException(
                what + " '" + value + "' must match " + pattern.pattern());
        }
        return value;
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load().getConfig("analytics_cdc");
        String baseUrl = trimSlash(config.getString("sccal_base_url"));
        Duration pollInterval = config.getDuration("poll_interval");

        log.info("SCCAL reference sync — API: {}", baseUrl);

        HttpClient http = SccalHttp.newClient();

        if (isOneShot(pollInterval)) {
            try (Connection conn = SqlScripts.bootstrap(config);
                 Statement st = conn.createStatement()) {
                runOnce(conn, st, http, baseUrl);
                log.info("SCCAL reference sync complete.");
            }
        } else {
            poll(config, http, baseUrl, pollInterval);
        }
    }

    /** A zero or negative {@code poll_interval} means run once and exit. */
    static boolean isOneShot(Duration pollInterval) {
        return pollInterval.isZero() || pollInterval.isNegative();
    }

    /**
     * Repeatedly capture on a fixed interval, surviving per-cycle failures.
     * Owns the DuckDB connection so it can rebuild it: a failed cycle can
     * abort the DuckLake catalog transaction in a way an in-place rollback
     * cannot always clear, so an unusable connection is re-bootstrapped before
     * the next cycle (a fresh ATTACH gives a fresh catalog connection) rather
     * than wedging the poller permanently.
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
            // The failure may have left autoCommit off mid-transaction; reset
            // so stale state can't leak into the next cycle's writes.
            resetConnection(conn);
        }
    }

    /** Best-effort: abandon any open transaction and restore autoCommit after a failed cycle. */
    static void resetConnection(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // Already in autoCommit mode, or unusable — nothing to roll back.
        }
        try {
            conn.setAutoCommit(true);
        } catch (SQLException ignored) {
            // The next cycle will surface a hard failure.
        }
    }

    /**
     * One full capture pass: the change-stream pull ({@link ChangeStreamSync}),
     * then the catalogue /list pull ({@link CatalogueListSync}) — each in its
     * own transaction, so a catalogue failure never rolls back committed
     * stream data. The stream's /cursors page carries the global-catalog gate
     * revision the catalogue pass keys off.
     */
    static void runOnce(Connection conn, Statement st, HttpClient http,
                        String baseUrl) throws SQLException {
        Long gateRevision = ChangeStreamSync.run(conn, st, http, baseUrl);
        CatalogueListSync.run(conn, st, http, baseUrl, gateRevision, Instant.now());
    }

    // ---- small helpers -------------------------------------------------------

    static String columns(List<Col> cols) {
        return join(cols, Col::column, ", ");
    }

    static <T> String join(List<T> items, Function<T, String> render, String sep) {
        return items.stream().map(render).collect(Collectors.joining(sep));
    }

    private static String trimSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private SccalReferenceSync() {
    }
}
