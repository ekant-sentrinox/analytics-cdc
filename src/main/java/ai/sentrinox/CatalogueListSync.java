package ai.sentrinox;

import ai.sentrinox.ChangeStreamSync.Counts;
import ai.sentrinox.SccalReferenceSync.Col;
import ai.sentrinox.SccalReferenceSync.MergeCounts;
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
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static ai.sentrinox.SccalReferenceSync.changedPredicate;
import static ai.sentrinox.SccalReferenceSync.columnDefs;
import static ai.sentrinox.SccalReferenceSync.columns;
import static ai.sentrinox.SccalReferenceSync.displayName;
import static ai.sentrinox.SccalReferenceSync.join;
import static ai.sentrinox.SccalReferenceSync.keyEq;
import static ai.sentrinox.SccalReferenceSync.notNull;
import static ai.sentrinox.SccalReferenceSync.setClause;
import static ai.sentrinox.SccalReferenceSync.stageName;

/**
 * Capture of the global-tier dimensions — the catalogue tables (providers,
 * models, MCP servers/tools) plus tenant — via the SCCAL {@code /list}
 * endpoint:
 *
 * <pre>
 *   GET /internal/sccal/api/v1/{objectType}/list?customerId=..&amp;tenantId=..[&amp;updatedSince=..]
 * </pre>
 *
 * <p>The global catalog is deliberately not in the entity-change stream (see
 * the admin layer's {@code global-catalog-sync.md}): its rows change only when
 * a global Flyway migration lands, so it syncs by snapshot + incremental pull.
 * The response is a JSON map {@code id → {…fields…, isDeleted}} — empty
 * {@code {}} when nothing matches — with soft-delete tombstones flagged
 * {@code isDeleted:true}; there is no pagination and no {@code updatedAt}
 * field in the payload.
 *
 * <p><b>Gate.</b> Global pulls are skipped while the {@code globalCatalog
 * .schemaRevision} gate (embedded in every /cursors page, extracted by
 * {@link ChangeStreamSync}) matches the stored revision (V7) — a near-free
 * check that keeps idle cycles off the /list endpoints. A null gate (server
 * predating it) falls back to pulling every cycle, which is correct, just
 * unoptimized. The gate is TTL-cached upstream (~2 min), which can only delay
 * a pull, never lose one: the cursor does not advance without a pull.
 *
 * <p><b>Cursor.</b> Per objectType, the {@code updatedSince} for the next
 * delta pull (V7). No cursor row → full live snapshot (no {@code
 * updatedSince}; the server omits tombstones there). After a non-empty pull
 * the cursor advances to <i>request start minus {@link #CURSOR_LAP}</i> —
 * the payload has no {@code updatedAt} to take a max over — and never moves
 * backwards. The lap absorbs consumer-vs-source clock skew; re-pulling the
 * overlap is harmless because the server bound is strict ({@code updated_at
 * &gt; since}) and the merge is idempotent. An empty delta writes nothing, so
 * an idle cycle commits no DuckLake snapshot.
 *
 * <p><b>Tenant.</b> {@code tenant/list} is snapshot-only upstream (its source
 * table has no updated_at/deleted_at; {@code updatedSince} is ignored) and
 * answers the single row of the {@code (customerId, tenantId)} pair passed —
 * so it is pulled per pair known to the change stream (V7 cursor rows), every
 * cycle, not gated by the catalog revision (tenant data is per-customer, not
 * global). Replays no-op in the merge, so this costs HTTP calls but no lake
 * writes. Tenant rows carry {@code isDeleted:false} always — a removed tenant
 * is never tombstoned, it just stops being pulled.
 *
 * <p>The declared tables live in config ({@code analytics_cdc.catalogue_syncs})
 * and apply in declaration order — keep referenced-before-referencing (FK)
 * order: providers before models, MCP servers before tools.
 */
final class CatalogueListSync {

    private static final Logger log = LoggerFactory.getLogger(CatalogueListSync.class);

    static final String CURSOR_TABLE = "ollylake.main.sccal_catalogue_cursor";
    static final String REVISION_TABLE = "ollylake.main.sccal_catalogue_revision";

    /**
     * Overlap subtracted from the request-start instant when advancing a
     * cursor. The /list payload carries no {@code updatedAt}, so the consumer
     * clock is the only cursor source; the lap covers consumer-vs-source
     * clock skew (the source stamps {@code updated_at} with its own
     * transaction time). Rows in the overlap window are re-pulled for up to a
     * lap after they change and re-merged idempotently.
     */
    static final Duration CURSOR_LAP = Duration.ofSeconds(60);

    /** objectType as the API spells it — lowercase, no separators. */
    private static final Pattern OBJECT_TYPE = Pattern.compile("[a-z][a-z0-9]*");

    /** URL-safe context value, spliced into request query strings. */
    private static final Pattern CONTEXT_VALUE = Pattern.compile("[A-Za-z0-9._-]+");

    /**
     * The customerId/tenantId sent on global catalogue pulls. Both params are
     * required by the endpoint but inert for global catalogue types
     * (catalog queries hit the {@code global.*} schema regardless), so any
     * valid value works; config makes it explicit and overridable.
     */
    record Context(String customerId, String tenantId) {
    }

    /**
     * One /list-fed dimension table. Global syncs ({@code perTenant} false)
     * pull once per gate change with the config context and no customer_id
     * column; the per-tenant sync pulls once per known (customer, tenant)
     * pair and stamps {@code customer_id} from the pair, exactly like the
     * change-stream path. The merge fragments (key match, SET clause, change
     * detection, NOT-NULL guards) are the shared builders on
     * {@link SccalReferenceSync}, so both capture paths keep one semantics.
     */
    record CatalogueSync(String objectType, String table, List<Col> keyCols,
                         List<Col> dataCols, boolean perTenant) {

        private List<Col> allCols() {
            return SccalReferenceSync.allCols(keyCols, dataCols);
        }

        /** Temp staging table for /list entries, unique per target table. */
        String stage() {
            return stageName("cat_", table);
        }

        String createStageSql() {
            String customer = perTenant ? "customer_id BIGINT, " : "";
            return "CREATE OR REPLACE TEMP TABLE " + stage() + " (" + customer
                + columnDefs(allCols()) + ", __is_deleted BOOLEAN)";
        }

        /**
         * Stage one /list body — a JSON map {@code id → entity} — via the
         * {@code $.*} object wildcard (zero rows for {@code {}}). Two
         * projections on purpose: the inner one materializes every field
         * access as raw text, the outer one applies the typed casts against
         * those plain columns, never {@code e} — the same DuckDB planner-bug
         * sidestep as the change-stream staging.
         *
         * @param customerId stamped onto every row for a per-tenant sync
         *                   (it is the poll context, not in the payload);
         *                   ignored for a global sync
         */
        String stageListSql(long customerId) {
            String insertCols = (perTenant ? "customer_id, " : "") + columns(allCols())
                + ", __is_deleted";
            String stamp = perTenant ? customerId + ", " : "";
            return "INSERT INTO " + stage() + " (" + insertCols + ") SELECT " + stamp
                + join(allCols(), Col::castExpr, ", ")
                + ", coalesce(TRY_CAST(__is_deleted AS BOOLEAN), false) AS __is_deleted FROM ("
                + "SELECT " + join(allCols(), Col::rawExpr, ", ")
                + ", e->>'isDeleted' AS __is_deleted FROM ("
                + "SELECT unnest(json_extract(?::JSON, '$.*')) AS e))";
        }

        /** Staging-vs-target key match; customer-scoped for a per-tenant sync. */
        private String keyMatch() {
            return keyEq(keyCols, perTenant);
        }

        /**
         * Soft-delete: flip {@code is_deleted} on the live rows a tombstone
         * targets and keep them, so joins still resolve a retired catalogue
         * row's name. Replayed tombstones no-op on the {@code is_deleted =
         * false} guard; a tombstone for a never-captured key matches nothing
         * and is dropped.
         */
        String softDeleteSql() {
            return "UPDATE " + table + " t SET is_deleted = true FROM " + stage()
                + " s WHERE " + keyMatch() + " AND s.__is_deleted AND t.is_deleted = false";
        }

        /**
         * Update the live attributes of matched rows and revive a previously
         * tombstoned one. Same NOT-NULL guard as the change-stream path: a
         * row missing a declared column is skipped (see {@link #applyStaged}),
         * never written — the V6 targets declare their columns NOT NULL and a
         * violation would abort the transaction.
         */
        String updateSql() {
            String notNullGuard = dataCols.isEmpty() ? "" : " AND " + notNull(dataCols);
            return "UPDATE " + table + " t SET " + setClause(dataCols) + " FROM " + stage()
                + " s WHERE " + keyMatch() + " AND NOT s.__is_deleted" + notNullGuard
                + " AND (" + changedPredicate(dataCols) + ")";
        }

        String insertSql() {
            String cols = (perTenant ? "customer_id, " : "") + columns(allCols());
            return "INSERT INTO " + table + " (" + cols + ") SELECT " + cols + " FROM "
                + stage() + " s WHERE NOT s.__is_deleted AND " + notNull(allCols())
                + " AND NOT EXISTS (SELECT 1 FROM " + table + " t WHERE " + keyMatch() + ")";
        }

        /**
         * Apply one staged /list body (soft-DELETE / UPDATE / INSERT).
         * Idempotent — a replayed body converges. Non-tombstone rows with a
         * NULL column — absent OR unparseable after TRY_CAST — are warned
         * about and skipped, exactly like the change-stream path: applying
         * one would abort the transaction and refail on the same body every
         * cycle. (A tombstone only needs its key; one with a NULL key matches
         * nothing and drops out silently.)
         */
        MergeCounts applyStaged(Statement st) throws SQLException {
            long skipped = SqlScripts.queryLong(st, "SELECT count(*) FROM " + stage()
                + " s WHERE NOT s.__is_deleted AND NOT (" + notNull(allCols()) + ")");
            if (skipped > 0) {
                log.warn("catalogue sync: {} {} entr(y/ies) skipped — payload missing or"
                    + " malformed for a required column", skipped, displayName(table));
            }
            int deleted = st.executeUpdate(softDeleteSql());
            int updated = st.executeUpdate(updateSql());
            int inserted = st.executeUpdate(insertSql());
            return new MergeCounts(inserted, updated, deleted);
        }
    }

    // The captured catalogue tables and the pull context, declared in config —
    // the single source of truth; adding a table is a DDL migration + config
    // block, no Java change (mirrors SccalReferenceSync.SYNCS).
    static final List<CatalogueSync> SYNCS;
    static final Context CONTEXT;

    /** Whether any declared sync is per-tenant — decides if the pairs table is read at all. */
    private static final boolean HAS_PER_TENANT;

    static {
        Config config = ConfigFactory.load().getConfig("analytics_cdc");
        SYNCS = loadCatalogueSyncs(config);
        CONTEXT = loadContext(config);
        HAS_PER_TENANT = SYNCS.stream().anyMatch(CatalogueSync::perTenant);
    }

    /**
     * Build the {@link CatalogueSync} declarations, failing fast on a malformed
     * entry — the list-level checks and error wrapping are shared with the
     * stream loader ({@link SccalReferenceSync#loadSyncList}).
     */
    static List<CatalogueSync> loadCatalogueSyncs(Config config) {
        return SccalReferenceSync.loadSyncList(config, "catalogue_syncs",
            CatalogueListSync::parseSync, CatalogueSync::table);
    }

    private static CatalogueSync parseSync(Config c) {
        String objectType = SccalReferenceSync.require(
            OBJECT_TYPE, c.getString("object_type"), "object_type");
        boolean perTenant = c.hasPath("per_tenant") && c.getBoolean("per_tenant");
        return new CatalogueSync(objectType, c.getString("table"),
            SccalReferenceSync.parseKeyCols(c), SccalReferenceSync.parseDataCols(c), perTenant);
    }

    static Context loadContext(Config config) {
        Config c = config.getConfig("catalogue_context");
        return new Context(
            SccalReferenceSync.require(CONTEXT_VALUE, c.getString("customer_id"),
                "catalogue_context.customer_id"),
            SccalReferenceSync.require(CONTEXT_VALUE, c.getString("tenant_id"),
                "catalogue_context.tenant_id"));
    }

    /** "Never stored" sentinel for the gate revision (real values are >= 0). */
    private static final long NO_REVISION = Long.MIN_VALUE;

    /**
     * One catalogue pull cycle (see class doc): global syncs when the gate
     * revision moved (or is unknown), the per-tenant sync for every known
     * pair, all in one transaction — committed only when something actually
     * changed, so an idle cycle writes no DuckLake snapshot.
     *
     * @param gateRevision {@code globalCatalog.schemaRevision} from this
     *                     cycle's /cursors page; null when the server does
     *                     not embed the gate
     * @param now          the cycle's wall-clock instant (injected for tests);
     *                     cursor advances are derived from it
     */
    static void run(Connection conn, Statement st, HttpClient http, String baseUrl,
                    Long gateRevision, Instant now) throws SQLException {
        long storedRevision = readRevision(st);
        boolean pullGlobals = gateRevision == null || gateRevision != storedRevision;
        List<long[]> pairs = HAS_PER_TENANT ? readTenantPairs(st) : List.of();
        if (!pullGlobals && pairs.isEmpty()) {
            log.info("catalogue sync: idle (gate unchanged at revision {})", storedRevision);
            return;
        }

        Map<String, Instant> cursors = pullGlobals ? readCursors(st) : Map.of();
        SqlScripts.inTransaction(conn, () -> {
            Counts totals = new Counts();
            boolean stateDirty = false;
            for (CatalogueSync sync : SYNCS) {
                if (sync.perTenant()) {
                    for (long[] pair : pairs) {
                        pull(conn, st, http, listUrl(baseUrl, sync.objectType(),
                                String.valueOf(pair[0]), String.valueOf(pair[1]), null),
                            sync, pair[0], totals);
                    }
                } else if (pullGlobals) {
                    Instant since = cursors.get(sync.objectType());
                    int staged = pull(conn, st, http, listUrl(baseUrl, sync.objectType(),
                        CONTEXT.customerId(), CONTEXT.tenantId(), since), sync, 0L, totals);
                    if (staged > 0) {
                        writeCursor(st, sync.objectType(), later(since, now.minus(CURSOR_LAP)));
                        stateDirty = true;
                    }
                }
            }
            // pullGlobals with a non-null gate implies the revision moved.
            if (pullGlobals && gateRevision != null) {
                writeRevision(st, gateRevision);
                stateDirty = true;
            }
            boolean changed = stateDirty || totals.any();
            if (changed) {
                log.info("catalogue sync: ins={} upd={} del={} (gate revision {})",
                    totals.inserted, totals.updated, totals.deleted,
                    gateRevision == null ? "unknown" : gateRevision);
            } else {
                log.info("catalogue sync: nothing changed — nothing to commit");
            }
            return changed;
        });
    }

    /** GET one /list body, stage and merge it; returns the staged row count. */
    private static int pull(Connection conn, Statement st, HttpClient http, String url,
                            CatalogueSync sync, long customerId, Counts totals)
            throws SQLException {
        String body = SccalHttp.requireOk(SccalHttp.get(http, url));
        st.execute(sync.createStageSql());
        int staged = DuckJson.executeWithJson(conn, sync.stageListSql(customerId), body);
        if (staged > 0) {
            totals.addMerge(sync.applyStaged(st));
        }
        return staged;
    }

    private static String listUrl(String baseUrl, String objectType, String customerId,
                                  String tenantId, Instant updatedSince) {
        String url = baseUrl + SccalReferenceSync.API_PATH + objectType
            + "/list?customerId=" + customerId + "&tenantId=" + tenantId;
        return updatedSince == null ? url : url + "&updatedSince=" + updatedSince;
    }

    // ---- state (V7 tables) ----------------------------------------------------

    /** Saved gate revision; {@link #NO_REVISION} when never stored. */
    private static long readRevision(Statement st) throws SQLException {
        Long v = SqlScripts.queryNullableLong(st,
            "SELECT max(schema_revision) FROM " + REVISION_TABLE);
        return v == null ? NO_REVISION : v;
    }

    private static void writeRevision(Statement st, long revision) throws SQLException {
        SqlScripts.replaceRows(st, REVISION_TABLE, null, String.valueOf(revision));
    }

    /** objectType → saved updatedSince; a malformed row is dropped (re-snapshot), not fatal. */
    private static Map<String, Instant> readCursors(Statement st) throws SQLException {
        Map<String, Instant> cursors = new HashMap<>();
        try (ResultSet rs = st.executeQuery(
            "SELECT object_type, next_updated_since FROM " + CURSOR_TABLE)) {
            while (rs.next()) {
                String objectType = rs.getString(1);
                String value = rs.getString(2);
                try {
                    cursors.put(objectType, Instant.parse(value));
                } catch (DateTimeParseException e) {
                    log.warn("catalogue sync: unparseable cursor '{}' for {} — falling back"
                        + " to a full snapshot", value, objectType);
                }
            }
        }
        return cursors;
    }

    private static void writeCursor(Statement st, String objectType, Instant next)
            throws SQLException {
        // objectType is shape-validated at load; the instant renders ISO-8601.
        SqlScripts.replaceRows(st, CURSOR_TABLE, "object_type = '" + objectType + "'",
            "'" + objectType + "', '" + next + "'");
    }

    /** The later of a (possibly absent) saved cursor and a candidate — never move backwards. */
    private static Instant later(Instant saved, Instant candidate) {
        return (saved == null || candidate.isAfter(saved)) ? candidate : saved;
    }

    /** Every (customer, tenant) pair the change stream knows (V7 cursor rows). */
    private static List<long[]> readTenantPairs(Statement st) throws SQLException {
        List<long[]> pairs = new ArrayList<>();
        try (ResultSet rs = st.executeQuery("SELECT DISTINCT customer_id, tenant_id FROM "
            + ChangeStreamSync.CHANGE_CURSOR_TABLE + " ORDER BY customer_id, tenant_id")) {
            while (rs.next()) {
                pairs.add(new long[] {rs.getLong(1), rs.getLong(2)});
            }
        }
        return pairs;
    }

    private CatalogueListSync() {
    }
}
