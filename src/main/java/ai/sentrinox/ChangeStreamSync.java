package ai.sentrinox;

import ai.sentrinox.SccalReferenceSync.Sync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.sql.Connection;
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
 * Change-stream capture for every reference table via the two SCCAL polling
 * endpoints — the ONLY endpoints this job uses:
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
 * <p>There is no snapshot ({@code /list}) fallback. The two edge cases are
 * handled entirely from the stream:
 * <ul>
 *   <li><b>bootstrap</b> — a pair with no cursor row replays its change stream
 *       from the start ({@code startOffset = 1}) so CREATE/UPDATE/DELETE entries
 *       reconstruct current state; its cursor is then seeded from where the
 *       replay ended;</li>
 *   <li><b>410 Gone</b> — a saved offset fell below SCCAL's prune watermark; the
 *       stream can no longer replay it, so the cursor fast-forwards to the
 *       registry head and a warning is logged. Changes that occurred only inside
 *       the pruned window cannot be recovered and are captured only when the
 *       entity next changes.</li>
 * </ul>
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
     * the type→entityType mapping. Types with no entry (e.g. ACTIVATION) are
     * ignored at staging (but still audited).
     */
    static final Map<String, List<Sync>> SYNCS_BY_ENTITY_TYPE =
        SccalReferenceSync.STREAM_ENTITY_TYPES.entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(
                Map.Entry::getValue,
                e -> SccalReferenceSync.SYNCS.stream()
                    .filter(s -> s.objectType().equals(e.getKey())).toList()));

    /** One registry row: a tenant's position in its own change stream. */
    record TenantCursor(long customerId, long tenantId, long lastChangeId) {
    }

    /** All registry entries with {@code seq >= startOffset}, plus the offset to poll next. */
    record CursorPage(List<TenantCursor> entries, long nextOffset) {
    }

    /** One cycle's running tallies — the merge deltas plus audit rows appended — for the log line. */
    private static final class Counts {
        int inserted;
        int updated;
        int deleted;
        int audited;

        /** Add one sync's {@code {inserted, updated, deleted}} result (the order applyChanges returns). */
        void addMerge(int[] iud) {
            inserted += iud[0];
            updated += iud[1];
            deleted += iud[2];
        }

        /** True once anything was captured this cycle — the signal to commit rather than roll back. */
        boolean any() {
            return inserted + updated + deleted + audited > 0;
        }
    }

    /** {@code /changes} answered 410 Gone: the saved offset was pruned upstream. */
    static final class GoneException extends RuntimeException {
        private final long startOffset;

        GoneException(long customerId, long tenantId, long startOffset) {
            super("410 Gone: entity_change offset " + startOffset + " pruned for customerId="
                + customerId + ", tenantId=" + tenantId);
            this.startOffset = startOffset;
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
            log.info("change stream: {} pair(s) without a cursor — bootstrapping by replaying"
                + " each pair's change stream from the start", missing.size());
            bootstrapPairs(conn, st, http, baseUrl, pairs, missing);
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

        applyAdvancedTenants(conn, st, http, baseUrl, advanced, offsets, page.nextOffset(),
            page.entries().size() >= PAGE_LIMIT);
    }

    /**
     * Pull and apply each advanced tenant's delta, then persist the moved
     * cursors — one transaction, one DuckLake snapshot. When every advanced
     * row turns out to be already consumed, the transaction is rolled back:
     * committing would write a metadata-only snapshot, the very thing the
     * idle path avoids by not persisting the registry offset. But when the
     * registry page was full ({@code pageFull}), the same escape as the idle
     * path applies — without it a page-or-more backlog of already-consumed
     * in-scope rows would pin the registry offset and be re-paged in full on
     * every poll forever.
     *
     * <p>A 410 on one tenant does not abort the cycle: the pages consumed before
     * it stay committed, other tenants apply normally, and the affected pair's
     * cursor fast-forwards past the pruned range (see class doc — the pruned
     * changes cannot be recovered without a snapshot endpoint).
     */
    private static void applyAdvancedTenants(Connection conn, Statement st, HttpClient http,
                                             String baseUrl, List<TenantCursor> advanced,
                                             Map<String, Long> offsets, long nextSeqOffset,
                                             boolean pageFull) throws SQLException {
        SqlScripts.inTransaction(conn, () -> {
            Counts totals = new Counts();
            boolean cursorMoved = false;
            for (TenantCursor c : advanced) {
                long from = offsets.getOrDefault(key(c.customerId(), c.tenantId()), 1L);
                if (c.lastChangeId() < from) {
                    continue;                     // registry row already consumed
                }
                long next = pullOrFastForward(conn, st, http, baseUrl, c, from, totals, false);
                if (next > from) {
                    writeChangeOffset(st, c.customerId(), c.tenantId(), next);
                    cursorMoved = true;
                }
            }
            writeRegistryOffset(st, nextSeqOffset);
            boolean changed = cursorMoved || totals.any();
            if (changed) {
                log.info("change stream: {} tenant(s) advanced — ins={} upd={} del={} audit={}",
                    advanced.size(), totals.inserted, totals.updated, totals.deleted, totals.audited);
            } else if (pageFull) {
                log.info("change stream: advanced row(s) already consumed — advanced registry"
                    + " offset past a full page of stale rows");
            } else {
                log.info("change stream: advanced row(s) already consumed — nothing to commit");
            }
            return changed || pageFull;
        });
    }

    /**
     * Page {@code /changes} for one tenant from {@code from} until
     * {@code hasMore} is false; returns the offset to save (the final
     * {@code nextOffset}). Throws {@link GoneException} on HTTP 410.
     */
    private static long pullChanges(Connection conn, Statement st, HttpClient http, String baseUrl,
                                    TenantCursor c, long from, Counts totals) throws SQLException {
        // view=BOTH: the sccal payload feeds the reference-table merge, the
        // human payload (full response DTO) is kept in the audit log.
        String urlPrefix = baseUrl + SccalReferenceSync.API_PATH + "changes"
            + "?customerId=" + c.customerId() + "&tenantId=" + c.tenantId() + "&view=BOTH&";
        return pageAll(conn, http, urlPrefix, from,
            offset -> new GoneException(c.customerId(), c.tenantId(), offset),
            body -> {
                totals.audited += auditChangePage(conn, c, body, from);
                applyChangePage(conn, st, body, c.customerId(), totals);
            });
    }

    /**
     * {@link #pullChanges} for one tenant with the shared 410-Gone recovery: if
     * the saved offset was pruned upstream, the pages applied before the 410 stay
     * committed in the caller's transaction and the cursor fast-forwards to the
     * registry head + 1. The pruned window is unrecoverable without a snapshot
     * endpoint — a change inside it is captured only when the entity next changes.
     * The two callers (steady-state and {@code bootstrap}) differ only in the
     * warning wording, selected by the flag. Returns the offset to save.
     */
    private static long pullOrFastForward(Connection conn, Statement st, HttpClient http,
                                          String baseUrl, TenantCursor c, long from,
                                          Counts totals, boolean bootstrap) throws SQLException {
        try {
            return pullChanges(conn, st, http, baseUrl, c, from, totals);
        } catch (GoneException gone) {
            long ff = c.lastChangeId() + 1;
            if (bootstrap) {
                log.warn("change stream: offset 1 pruned for customerId={}, tenantId={}"
                    + " — bootstrapping at registry head {} (entities that changed only"
                    + " before the prune watermark are not captured until they next change)",
                    c.customerId(), c.tenantId(), ff);
            } else {
                log.warn("change stream: offset {} pruned for customerId={}, tenantId={}"
                    + " — fast-forwarding to {} (changes inside the pruned window are lost"
                    + " until the entity next changes)", gone.startOffset(),
                    c.customerId(), c.tenantId(), ff);
            }
            return ff;
        }
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

    /**
     * Stage one page's entries per fed sync and apply them (DELETE / UPDATE /
     * INSERT). {@code customerId} (the pair being pulled) is stamped onto every
     * staged row — it scopes the target table and is not in the entity payload.
     */
    private static void applyChangePage(Connection conn, Statement st, String body,
                                        long customerId, Counts totals) throws SQLException {
        for (Map.Entry<String, List<Sync>> byType : SYNCS_BY_ENTITY_TYPE.entrySet()) {
            for (Sync s : byType.getValue()) {
                st.execute(s.createChangeStageSql());
                int staged = DuckJson.executeWithJson(conn, s.stageChangesSql(byType.getKey(), customerId), body);
                // A page usually carries one entityType, so most syncs stage
                // nothing; the dedupe + DELETE/UPDATE/INSERT would all be no-ops
                // (adding {0,0,0}), so skip them when this sync staged no rows.
                if (staged > 0) {
                    totals.addMerge(s.applyChanges(st));
                }
            }
        }
    }

    // ---- bootstrap ------------------------------------------------------------

    /**
     * First-contact bootstrap for pairs with no cursor: replay each pair's
     * change stream from the start ({@code startOffset = 1}) so its
     * CREATE/UPDATE/DELETE entries reconstruct current state, then seed its
     * cursor from where the replay ended. If offset 1 was already pruned
     * upstream (410), fall back to the registry head and warn — the pre-prune
     * history cannot be recovered without a snapshot endpoint.
     *
     * <p>On a fresh bootstrap (every pair new) the registry offset fast-forwards
     * past the head so steady state doesn't re-page the rows just consumed; a
     * partial bootstrap leaves it so healthy pairs' pending registry rows stay
     * discoverable. The transaction always commits: seeding a cursor is real
     * progress (it is what stops the next cycle re-bootstrapping) even when the
     * stream carried no rows.
     */
    private static void bootstrapPairs(Connection conn, Statement st, HttpClient http, String baseUrl,
                                       List<long[]> pairs, List<long[]> missing) throws SQLException {
        CursorPage head = fetchCursors(conn, http, baseUrl, 1);
        Map<String, Long> heads = new HashMap<>();
        for (TenantCursor c : head.entries()) {
            heads.merge(key(c.customerId(), c.tenantId()), c.lastChangeId(), Math::max);
        }

        SqlScripts.inTransaction(conn, () -> {
            Counts totals = new Counts();
            for (long[] pair : missing) {
                long headId = heads.getOrDefault(key(pair[0], pair[1]), 0L);
                TenantCursor c = new TenantCursor(pair[0], pair[1], headId);
                long next = pullOrFastForward(conn, st, http, baseUrl, c, 1L, totals, true);
                writeChangeOffset(st, pair[0], pair[1], next);
            }
            // Only fast-forward the registry offset on a full bootstrap; a partial
            // one must leave healthy pairs' pending rows discoverable.
            if (missing.size() == pairs.size()) {
                writeRegistryOffset(st, head.nextOffset());
            }
            log.info("change stream: bootstrapped {} pair(s) from the stream — ins={} upd={}"
                + " del={} audit={}", missing.size(), totals.inserted, totals.updated,
                totals.deleted, totals.audited);
            return true;
        });
    }

    // ---- /cursors -------------------------------------------------------------

    /** Page the registry from {@code startOffset} until {@code hasMore} is false. */
    private static CursorPage fetchCursors(Connection conn, HttpClient http, String baseUrl,
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
            DuckJson.Page page = DuckJson.extractPage(conn, body, offset);
            if (page.nextOffset() <= offset) {
                return offset;
            }
            offset = page.nextOffset();
            if (!page.hasMore()) {
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
        return DuckJson.queryAll(conn, sql, body,
            rs -> new TenantCursor(rs.getLong(1), rs.getLong(2), rs.getLong(3)));
    }

    // ---- cursor state (V7 tables) ----------------------------------------------

    /** {@code customerId:tenantId} → next /changes startOffset, for every known pair. */
    private static Map<String, Long> readChangeOffsets(Statement st) throws SQLException {
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
    private static long readRegistryOffset(Statement st) throws SQLException {
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
