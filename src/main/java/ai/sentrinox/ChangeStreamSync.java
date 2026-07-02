package ai.sentrinox;

import ai.sentrinox.SccalReferenceSync.Sync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

/**
 * Change-stream capture for the stream-fed reference tables
 * ({@link SccalReferenceSync#STREAM_SYNCS}) via the two SCCAL polling endpoints:
 *
 * <pre>
 *   GET /internal/sccal/api/v1/cursors   — which (customer, tenant) advanced, and to where
 *   GET /internal/sccal/api/v1/changes   — that tenant's entity_change entries from an offset
 * </pre>
 *
 * <p>Each cycle is a two-level poll. Level 1 asks the global cursor registry
 * for tenants whose stream advanced past our saved registry offset — an idle
 * system answers with one empty page and the cycle ends without touching the
 * lake. Level 2 pages {@code /changes} for each advanced tenant from its saved
 * per-tenant offset and applies the entries (explicit CREATE / UPDATE / DELETE
 * actions — no full-snapshot diffing).
 *
 * <p>Every pulled {@code /changes} entry — all entity types, whether or not it
 * feeds a reference table — is also appended to the {@code entity_change_audit}
 * log ({@code V8}) with both payload views ({@code view=BOTH}: the minimal
 * sccal shape that drives the merge, and the full human response DTO).
 *
 * <p>Both cursor positions are persisted in the lake ({@code V7} tables) and
 * written <b>in the same transaction as the data they describe</b> — the audit
 * rows join it too — so a crash can only lose whole cycles (replayed
 * idempotently), never commit data without its cursor or vice versa.
 *
 * <p>The full-snapshot machinery remains the fallback, used in two cases:
 * <ul>
 *   <li><b>bootstrap</b> — a pair with no cursor row may predate SCCAL's change
 *       capture (its history is not in {@code entity_change}), so the run
 *       snapshots the stream-fed tables and seeds that pair's cursor from the
 *       registry head;</li>
 *   <li><b>410 Gone</b> — a saved offset fell below SCCAL's prune watermark;
 *       the stream can no longer replay it, so resync from snapshots and
 *       fast-forward that pair's cursor.</li>
 * </ul>
 * Both fetch all pairs, so "absent from snapshot" stays a safe delete signal
 * on the shared (non-tenant-scoped) reference tables — but only the affected
 * pair's cursor is reseeded: wiping a healthy pair's cursor would fast-forward
 * it past unconsumed changes and silently gap the audit log.
 */
final class ChangeStreamSync {

    private static final Logger log = LoggerFactory.getLogger(ChangeStreamSync.class);

    private static final String CHANGE_CURSOR_TABLE = "ollylake.main.sccal_change_cursor";
    private static final String REGISTRY_CURSOR_TABLE = "ollylake.main.sccal_registry_cursor";
    private static final String AUDIT_TABLE = "ollylake.main.entity_change_audit";
    private static final int PAGE_LIMIT = 1000;

    /**
     * {@code /changes} entityType → the sync(s) it feeds — derived from
     * {@link SccalReferenceSync#STREAM_ENTITY_TYPES}, the single declaration of
     * the stream/snapshot split. Types with no entry (e.g. ACTIVATION) are
     * ignored at staging.
     */
    static final Map<String, List<Sync>> SYNCS_BY_ENTITY_TYPE =
        SccalReferenceSync.STREAM_ENTITY_TYPES.entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(
                Map.Entry::getValue,
                e -> SccalReferenceSync.STREAM_SYNCS.stream()
                    .filter(s -> s.objectType().equals(e.getKey())).toList()));

    /** One registry row: a tenant's position in its own change stream. */
    record TenantCursor(long customerId, long tenantId, long lastChangeId) {
    }

    /** All registry entries with {@code seq >= startOffset}, plus the offset to poll next. */
    record CursorPage(List<TenantCursor> entries, long nextOffset) {
    }

    /** {@code /changes} answered 410 Gone: the saved offset was pruned upstream. */
    static final class GoneException extends RuntimeException {
        private final long customerId;
        private final long tenantId;
        private final long startOffset;

        GoneException(long customerId, long tenantId, long startOffset) {
            super("410 Gone: entity_change offset " + startOffset + " pruned for customerId="
                + customerId + ", tenantId=" + tenantId + " — resyncing from full snapshots");
            this.customerId = customerId;
            this.tenantId = tenantId;
            this.startOffset = startOffset;
        }

        long customerId() {
            return customerId;
        }

        long tenantId() {
            return tenantId;
        }

        /** The offset whose page answered 410 — pages before it were consumed intact. */
        long startOffset() {
            return startOffset;
        }
    }

    /** One change-stream cycle for the given capture scope (see class doc). */
    static void run(Connection conn, Statement st, HttpClient http, String baseUrl,
                    List<long[]> pairs) throws SQLException {
        Map<String, Long> offsets = readChangeOffsets(st);
        List<long[]> missing = pairs.stream()
            .filter(p -> !offsets.containsKey(key(p[0], p[1]))).toList();
        if (!missing.isEmpty()) {
            log.info("change stream: {} pair(s) without a cursor — bootstrapping stream-fed"
                + " tables from full snapshots", missing.size());
            resyncFromSnapshots(conn, st, http, baseUrl, pairs, missing);
            return;
        }

        long seqOffset = readRegistryOffset(st);
        CursorPage page = fetchCursors(conn, http, baseUrl, seqOffset);
        List<TenantCursor> advanced = dedupeByPair(inScope(page.entries(), pairs));
        if (advanced.isEmpty()) {
            // Nothing in scope advanced. A small out-of-scope backlog is
            // deliberately NOT persisted — refetching a few rows next cycle is
            // cheaper than a metadata-only lake commit. But once the backlog
            // exceeds a page it would otherwise grow (and be re-paged) forever,
            // so pay one commit to move the offset past it.
            if (page.entries().size() >= PAGE_LIMIT) {
                SqlScripts.inTransaction(conn, () -> {
                    writeRegistryOffset(st, page.nextOffset());
                    return true;
                });
                log.info("change stream: idle — advanced registry offset past {} out-of-scope"
                    + " row(s)", page.entries().size());
            } else {
                log.info("change stream: idle (no tenant advanced past seq {})", seqOffset);
            }
            return;
        }

        List<long[]> gonePairs = applyAdvancedTenants(conn, st, http, baseUrl, advanced,
            offsets, page.nextOffset(), page.entries().size() >= PAGE_LIMIT);
        if (!gonePairs.isEmpty()) {
            resyncFromSnapshots(conn, st, http, baseUrl, pairs, gonePairs);
        }
    }

    /**
     * Pull and apply each advanced tenant's delta, then persist the moved
     * cursors — one transaction, one DuckLake snapshot. When every advanced
     * row turns out to be already consumed, the transaction is rolled back:
     * committing would write a metadata-only snapshot, the very thing the
     * idle path avoids by not persisting the registry offset. But when the
     * registry page was full ({@code pageFull}), the same escape as the idle
     * path applies — without it a page-or-more backlog of already-consumed
     * in-scope rows (e.g. left behind by a partial 410 reseed) would pin the
     * registry offset and be re-paged in full on every poll forever.
     *
     * <p>A 410 on one tenant does not abort the cycle: the pages consumed
     * before it stay committed (with the cursor advanced up to the last
     * completed page, so their audit rows are not lost), other tenants apply
     * normally, and the affected pairs are returned for a snapshot resync.
     */
    private static List<long[]> applyAdvancedTenants(Connection conn, Statement st, HttpClient http,
                                                     String baseUrl, List<TenantCursor> advanced,
                                                     Map<String, Long> offsets, long nextSeqOffset,
                                                     boolean pageFull) throws SQLException {
        List<long[]> gonePairs = new ArrayList<>();
        SqlScripts.inTransaction(conn, () -> {
            int[] totals = new int[4];            // {ins, upd, del, audit rows}
            boolean cursorMoved = false;
            for (TenantCursor c : advanced) {
                long from = offsets.getOrDefault(key(c.customerId(), c.tenantId()), 1L);
                if (c.lastChangeId() < from) {
                    continue;                     // registry row already consumed
                }
                long next;
                try {
                    next = pullChanges(conn, st, http, baseUrl, c, from, totals);
                } catch (GoneException gone) {
                    // Pages before the pruned offset were applied and audited in
                    // this transaction; keep them by saving the cursor up to the
                    // last completed page instead of rolling the cycle back —
                    // the snapshot resync below covers everything past it.
                    log.warn(gone.getMessage());
                    gonePairs.add(new long[] {gone.customerId(), gone.tenantId()});
                    next = gone.startOffset();
                }
                if (next > from) {
                    writeChangeOffset(st, c.customerId(), c.tenantId(), next);
                    cursorMoved = true;
                }
            }
            writeRegistryOffset(st, nextSeqOffset);
            boolean changed = cursorMoved || totals[0] + totals[1] + totals[2] + totals[3] > 0;
            if (changed) {
                log.info("change stream: {} tenant(s) advanced — ins={} upd={} del={} audit={}",
                    advanced.size(), totals[0], totals[1], totals[2], totals[3]);
            } else if (pageFull) {
                log.info("change stream: advanced row(s) already consumed — advanced registry"
                    + " offset past a full page of stale rows");
            } else {
                log.info("change stream: advanced row(s) already consumed — nothing to commit");
            }
            return changed || pageFull;
        });
        return gonePairs;
    }

    /**
     * Page {@code /changes} for one tenant from {@code from} until
     * {@code hasMore} is false; returns the offset to save (the final
     * {@code nextOffset}). Throws {@link GoneException} on HTTP 410.
     */
    private static long pullChanges(Connection conn, Statement st, HttpClient http, String baseUrl,
                                    TenantCursor c, long from, int[] totals) throws SQLException {
        // view=BOTH: the sccal payload feeds the reference-table merge, the
        // human payload (full response DTO) is kept in the audit log.
        String urlPrefix = baseUrl + SccalReferenceSync.API_PATH + "changes"
            + "?customerId=" + c.customerId() + "&tenantId=" + c.tenantId() + "&view=BOTH&";
        return pageAll(conn, http, urlPrefix, from,
            offset -> new GoneException(c.customerId(), c.tenantId(), offset),
            body -> {
                totals[3] += auditChangePage(conn, c, body, from);
                applyChangePage(conn, st, body, totals);
            });
    }

    /**
     * Append one page's entries — ALL entity types, applied or not — to the
     * audit log; returns the number of rows appended. Joins the caller's open
     * transaction, so an audit row commits with the merge (and the cursor) it
     * belongs to. Idempotent: a replayed page's entries hit the
     * {@code (customer_id, tenant_id, change_id)} NOT EXISTS guard, bounded
     * below by {@code minChangeId} (the pull's start offset — no entry in the
     * page can have a smaller id) so the anti-join prunes the ever-growing
     * audit log instead of scanning all of it.
     *
     * <p>Numeric/timestamp fields use TRY_CAST and entries missing a NOT NULL
     * audit column (id, entityType, action) are skipped with a warning: one
     * malformed entry must not abort the transaction — that would pin the
     * cursor and refail on the same page every cycle.
     */
    private static int auditChangePage(Connection conn, TenantCursor c, String body,
                                       long minChangeId) throws SQLException {
        // The fields are materialized in an inner projection before the guards
        // apply: combining several `entry->>'f' IS NULL` predicates directly
        // over the unnest column trips a DuckDB (1.4.5) planner conversion bug.
        String shredded = "SELECT TRY_CAST(entry->>'id' AS BIGINT) AS change_id,"
            + " entry->>'entityType' AS entity_type,"
            + " TRY_CAST(entry->>'entityId' AS BIGINT) AS entity_id,"
            + " entry->>'action' AS action,"
            + " TRY_CAST(entry->>'commitId' AS BIGINT) AS commit_id,"
            + " TRY_CAST(entry->>'changedAt' AS TIMESTAMPTZ) AS changed_at,"
            + " entry->>'sccal' AS sccal_payload, entry->>'human' AS human_payload"
            + " FROM (SELECT unnest(json_extract(?::JSON, '$.entries[*]')) AS entry)";
        long malformed = DuckJson.queryLong(conn,
            "SELECT count(*) FROM (" + shredded + ")"
                + " WHERE change_id IS NULL OR entity_type IS NULL OR action IS NULL", body);
        if (malformed > 0) {
            log.warn("change stream: {} malformed /changes entr(y/ies) for customerId={},"
                    + " tenantId={} skipped (missing id/entityType/action) — not auditable"
                    + " or applicable", malformed, c.customerId(), c.tenantId());
        }
        String sql = "INSERT INTO " + AUDIT_TABLE
            + " (change_id, customer_id, tenant_id, entity_type, entity_id, action,"
            + " commit_id, changed_at, sccal_payload, human_payload)"
            + " SELECT change_id, " + c.customerId() + ", " + c.tenantId() + ","
            + " entity_type, entity_id, action, commit_id, changed_at,"
            + " sccal_payload, human_payload"
            + " FROM (" + shredded + ") e"
            + " WHERE e.change_id IS NOT NULL AND e.entity_type IS NOT NULL"
            + " AND e.action IS NOT NULL"
            + " AND NOT EXISTS (SELECT 1 FROM " + AUDIT_TABLE + " a"
            + " WHERE a.customer_id = " + c.customerId()
            + " AND a.tenant_id = " + c.tenantId()
            + " AND a.change_id >= " + minChangeId
            + " AND a.change_id = e.change_id)";
        return DuckJson.executeWithJson(conn, sql, body);
    }

    /** Stage one page's entries per fed sync and apply them (DELETE / UPDATE / INSERT). */
    private static void applyChangePage(Connection conn, Statement st, String body, int[] totals)
            throws SQLException {
        for (Map.Entry<String, List<Sync>> byType : SYNCS_BY_ENTITY_TYPE.entrySet()) {
            for (Sync s : byType.getValue()) {
                st.execute(s.createChangeStageSql());
                DuckJson.executeWithJson(conn, s.stageChangesSql(byType.getKey()), body);
                SccalReferenceSync.addCounts(totals, s.applyChanges(st));
            }
        }
    }

    // ---- bootstrap / 410 resync ----------------------------------------------

    /**
     * Capture the stream-fed tables from full snapshots (all pairs, so absence
     * is a safe delete signal), then seed the cursors of {@code reseedPairs} —
     * the pairs that actually need it: the ones with no cursor row (bootstrap)
     * or whose offset was pruned (410 Gone) — from the registry head, merge and
     * cursor writes in one transaction. The snapshot supersedes those pairs'
     * pending changes, so fast-forwarding their cursors is safe; every other
     * pair keeps its cursor, so its unconsumed changes still reach the audit
     * log on a later cycle. The registry offset fast-forwards only when every
     * pair was reseeded (a fresh bootstrap): skipping ahead while healthy
     * cursors trail would hide their pending registry rows from discovery.
     */
    static void resyncFromSnapshots(Connection conn, Statement st, HttpClient http,
                                    String baseUrl, List<long[]> pairs,
                                    List<long[]> reseedPairs) throws SQLException {
        CursorPage head = fetchCursors(conn, http, baseUrl, 1);
        Map<String, Long> lastChangeIds = new HashMap<>();
        for (TenantCursor c : head.entries()) {
            lastChangeIds.merge(key(c.customerId(), c.tenantId()), c.lastChangeId(), Math::max);
        }

        List<Sync> syncs = SccalReferenceSync.STREAM_SYNCS;
        long staged = SccalReferenceSync.stageAll(conn, st, http, baseUrl, pairs, syncs);

        // A snapshot that staged zero rows across every pair is ambiguous — a
        // genuinely empty tenant looks the same as a blank/degenerate-but-200
        // body (coalesced to "{}"). Seeding cursors over it would mark the pair
        // bootstrapped while capturing nothing, permanently skipping its data
        // (nothing ever re-bootstraps a pair that has a cursor row). Mirror the
        // merge path's empty-stage guard: skip the cursor writes and let the
        // next cycle retry the bootstrap/resync.
        boolean seedCursors = staged > 0;
        if (!seedCursors) {
            log.warn("change stream: snapshot staged 0 rows — not seeding cursors"
                + " (blank response indistinguishable from an empty tenant); retrying next cycle");
        }

        SccalReferenceSync.printCaptureHeader();
        try {
            SqlScripts.inTransaction(conn, () -> {
                int[] totals = SccalReferenceSync.mergeAll(st, syncs);
                if (seedCursors) {
                    for (long[] pair : reseedPairs) {
                        long next = lastChangeIds.getOrDefault(key(pair[0], pair[1]), 0L) + 1;
                        writeChangeOffset(st, pair[0], pair[1], next);
                    }
                    if (reseedPairs.size() == pairs.size()) {
                        writeRegistryOffset(st, head.nextOffset());
                    }
                }
                SccalReferenceSync.printCaptureTotals(totals);
                // Commit whenever cursors were seeded: even with zero row changes
                // they are real progress (they are what stops the next cycle
                // re-bootstrapping). With nothing staged the merge is a no-op, so
                // roll back rather than write an empty DuckLake snapshot.
                return seedCursors;
            });
        } finally {
            SccalReferenceSync.dropStages(st, syncs);
        }
    }

    // ---- /cursors -------------------------------------------------------------

    /** Page the registry from {@code startOffset} until {@code hasMore} is false. */
    static CursorPage fetchCursors(Connection conn, HttpClient http, String baseUrl,
                                   long startOffset) throws SQLException {
        List<TenantCursor> all = new ArrayList<>();
        String urlPrefix = baseUrl + SccalReferenceSync.API_PATH + "cursors?";
        long next = pageAll(conn, http, urlPrefix, startOffset, null,
            body -> all.addAll(parseCursors(conn, body)));
        return new CursorPage(all, next);
    }

    /** Consumes one page body of an offset-paged endpoint. */
    @FunctionalInterface
    private interface PageHandler {
        void page(String body) throws SQLException;
    }

    /**
     * Drive one offset-paged SCCAL endpoint: GET
     * {@code urlPrefix + "startOffset=…&limit=…"} from {@code startOffset}
     * until {@code hasMore} is false, feeding each body to {@code handler};
     * returns the offset to save (the final {@code nextOffset}). A page that
     * makes no progress ({@code nextOffset <= offset}) stops the paging
     * defensively. {@code on410}, when non-null, maps the current offset to
     * the exception thrown on HTTP 410; otherwise 410 fails like any non-200.
     */
    private static long pageAll(Connection conn, HttpClient http, String urlPrefix,
                                long startOffset, LongFunction<RuntimeException> on410,
                                PageHandler handler) throws SQLException {
        long offset = startOffset;
        while (true) {
            String url = urlPrefix + "startOffset=" + offset + "&limit=" + PAGE_LIMIT;
            HttpResponse<String> resp = SccalHttp.get(http, url);
            if (on410 != null && resp.statusCode() == 410) {
                throw on410.apply(offset);
            }
            String body = SccalHttp.requireOk(resp);
            handler.page(body);
            long nextOffset = DuckJson.extractLong(conn, body, "nextOffset", offset);
            boolean hasMore = DuckJson.extractBool(conn, body, "hasMore");
            if (nextOffset <= offset) {
                return offset;
            }
            offset = nextOffset;
            if (!hasMore) {
                return offset;
            }
        }
    }

    /** Shred one /cursors page with DuckDB (ids arrive as JSON strings), in seq order. */
    private static List<TenantCursor> parseCursors(Connection conn, String body) throws SQLException {
        String sql = "SELECT (e->>'customerId')::BIGINT, (e->>'tenantId')::BIGINT,"
            + " coalesce((e->>'lastChangeId')::BIGINT, 0)"
            + " FROM (SELECT unnest(json_extract(?::JSON, '$.entries[*]')) AS e)"
            + " ORDER BY (e->>'seq')::BIGINT";
        List<TenantCursor> cursors = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, body);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cursors.add(new TenantCursor(rs.getLong(1), rs.getLong(2), rs.getLong(3)));
                }
            }
        }
        return cursors;
    }

    // ---- cursor state (V7 tables) ----------------------------------------------

    /** {@code customerId:tenantId} → next /changes startOffset, for every known pair. */
    static Map<String, Long> readChangeOffsets(Statement st) throws SQLException {
        Map<String, Long> offsets = new HashMap<>();
        try (ResultSet rs = st.executeQuery(
            "SELECT customer_id, tenant_id, next_change_offset FROM " + CHANGE_CURSOR_TABLE)) {
            while (rs.next()) {
                offsets.put(key(rs.getLong(1), rs.getLong(2)), rs.getLong(3));
            }
        }
        return offsets;
    }

    private static void writeChangeOffset(Statement st, long customerId, long tenantId,
                                          long next) throws SQLException {
        st.execute("DELETE FROM " + CHANGE_CURSOR_TABLE + " WHERE customer_id = " + customerId
            + " AND tenant_id = " + tenantId);
        st.execute("INSERT INTO " + CHANGE_CURSOR_TABLE + " VALUES ("
            + customerId + ", " + tenantId + ", " + next + ")");
    }

    /** Saved /cursors startOffset; 1 when never saved. */
    static long readRegistryOffset(Statement st) throws SQLException {
        try (ResultSet rs = st.executeQuery(
            "SELECT max(next_seq_offset) FROM " + REGISTRY_CURSOR_TABLE)) {
            rs.next();
            long v = rs.getLong(1);
            return (rs.wasNull() || v < 1) ? 1 : v;
        }
    }

    private static void writeRegistryOffset(Statement st, long next) throws SQLException {
        st.execute("DELETE FROM " + REGISTRY_CURSOR_TABLE);
        st.execute("INSERT INTO " + REGISTRY_CURSOR_TABLE + " VALUES (" + next + ")");
    }

    // ---- small helpers ----------------------------------------------------------

    /**
     * Registry rows for pairs outside {@code customer_tenant_reference} are
     * ignored: that table defines the capture scope (see the invariant on
     * {@code readCustomerTenantPairs}).
     */
    private static List<TenantCursor> inScope(List<TenantCursor> entries, List<long[]> pairs) {
        Set<String> scope = pairs.stream().map(p -> key(p[0], p[1])).collect(Collectors.toSet());
        return entries.stream()
            .filter(c -> scope.contains(key(c.customerId(), c.tenantId()))).toList();
    }

    /**
     * Collapse duplicate registry rows per pair to the furthest advance, so a
     * tenant that advanced several times since our offset is pulled once, not
     * once per registry row (each re-pull would restart from the same saved
     * offset and redo the whole range).
     */
    private static List<TenantCursor> dedupeByPair(List<TenantCursor> entries) {
        Map<String, TenantCursor> byPair = new LinkedHashMap<>();
        for (TenantCursor c : entries) {
            byPair.merge(key(c.customerId(), c.tenantId()), c,
                (a, b) -> b.lastChangeId() > a.lastChangeId() ? b : a);
        }
        return List.copyOf(byPair.values());
    }

    private static String key(long customerId, long tenantId) {
        return customerId + ":" + tenantId;
    }

    private ChangeStreamSync() {
    }
}
