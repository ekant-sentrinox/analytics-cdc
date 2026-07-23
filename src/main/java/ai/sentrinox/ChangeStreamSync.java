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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;

/**
 * Change-stream capture for every reference table via the two SCCAL polling
 * endpoints — the ONLY endpoints this job uses:
 *
 * <pre>
 *   GET /internal/sccal/api/v1/cursors   — which (customer, tenant) advanced, and to where
 *   GET /internal/sccal/api/v1/changes   — that tenant's entity_change entries from an offset
 * </pre>
 *
 * <p>Each cycle is a two-level poll: level 1 asks the global cursor registry
 * for tenants whose stream advanced past our saved registry offset (an idle
 * system answers with one empty page and the cycle ends without touching the
 * lake); level 2 pages {@code /changes} for each advanced tenant from its
 * saved offset and applies the explicit CREATE / UPDATE / DELETE entries.
 *
 * <p>Every pulled entry — whether or not it feeds a reference table — is also
 * appended to the {@code entity_change_audit} log (V8) with both payload views
 * ({@code view=BOTH}).
 *
 * <p>Both cursor positions are persisted in the lake (V7) and written <b>in the
 * same transaction as the data they describe</b> — the audit rows join it too —
 * so a crash can only lose whole cycles (replayed idempotently), never commit
 * data without its cursor or vice versa.
 *
 * <p>The registry itself defines the capture scope: every {@code (customerId,
 * tenantId)} pair it reports is captured, so a new tenant is picked up the
 * first time it appears. There is no snapshot ({@code /list}) fallback; the
 * two edge cases are handled entirely from the stream:
 * <ul>
 *   <li><b>bootstrap</b> — a pair with no saved cursor replays its change
 *       stream from the start ({@code startOffset = 1}), reconstructing
 *       current state;</li>
 *   <li><b>410 Gone</b> — a saved offset fell below SCCAL's prune watermark,
 *       so the cursor fast-forwards to the registry head. Changes only inside
 *       the pruned window are captured when the entity next changes.</li>
 * </ul>
 */
final class ChangeStreamSync {

    private static final Logger log = LoggerFactory.getLogger(ChangeStreamSync.class);

    /** Also read by {@link CatalogueListSync} to discover the known (customer, tenant) pairs. */
    static final String CHANGE_CURSOR_TABLE = "ollylake.main.sccal_change_cursor";
    private static final String REGISTRY_CURSOR_TABLE = "ollylake.main.sccal_registry_cursor";
    private static final String AUDIT_TABLE = "ollylake.main.entity_change_audit";
    private static final String ENTRIES_STAGE = SccalReferenceSync.ENTRIES_STAGE;
    private static final int PAGE_LIMIT = 1000;

    /**
     * Defensive ceiling per {@link #pageAll} call: an endpoint that keeps
     * answering {@code hasMore:true} with advancing offsets must not loop one
     * transaction forever — progress is saved and the pull resumes next cycle.
     */
    private static final int MAX_PAGES_PER_PULL = 10_000;

    /** One registry row: a tenant's position in its own change stream. */
    record TenantCursor(long customerId, long tenantId, long lastChangeId) {
    }

    /**
     * All registry entries with {@code seq >= startOffset}, plus the offset to
     * poll next and the embedded global-catalog gate revision
     * ({@code globalCatalog.schemaRevision} — present on every page, including
     * an idle one; null when the server predates the gate).
     */
    record CursorPage(List<TenantCursor> entries, long nextOffset, Long schemaRevision) {
    }

    /**
     * One cycle's running tallies — the merge deltas plus audit rows appended —
     * for the log line and the commit decision. Shared with
     * {@link CatalogueListSync} (which never audits).
     */
    static final class Counts {
        int inserted;
        int updated;
        int deleted;
        int audited;

        void addMerge(SccalReferenceSync.MergeCounts merge) {
            inserted += merge.inserted();
            updated += merge.updated();
            deleted += merge.deleted();
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

    /**
     * One change-stream cycle (see class doc). Pairs are discovered from the
     * registry itself: a reported pair with no saved cursor starts from offset 1
     * (first-contact bootstrap), all others resume from their saved offset.
     *
     * @return the global-catalog gate revision embedded in the /cursors page
     *         ({@code globalCatalog.schemaRevision}) — the catalogue pull
     *         ({@link CatalogueListSync}) keys off it; null when the server
     *         does not embed the gate
     */
    static Long run(Connection conn, Statement st, HttpClient http, String baseUrl)
            throws SQLException {
        long seqOffset = readRegistryOffset(st);
        CursorPage page = fetchCursors(conn, http, baseUrl, seqOffset);
        List<TenantCursor> advanced = dedupeByPair(page.entries());
        if (advanced.isEmpty()) {
            log.info("change stream: idle (no tenant advanced past seq {})", seqOffset);
            return page.schemaRevision();
        }

        // Read only once a tenant actually advanced — the idle cycle above
        // must not scan the cursor table.
        Map<String, Long> offsets = readChangeOffsets(st);
        applyAdvancedTenants(conn, st, http, baseUrl, advanced, offsets, page.nextOffset(),
            page.entries().size() >= PAGE_LIMIT);
        return page.schemaRevision();
    }

    /**
     * Pull and apply each advanced tenant's delta, then persist the moved
     * cursors — one transaction, one DuckLake snapshot. When every advanced
     * row was already consumed, the transaction is rolled back (committing
     * would write a metadata-only snapshot) — unless the registry page was
     * full ({@code pageFull}), where the offset must advance anyway or a
     * page-sized backlog of stale rows would pin it and be re-paged in full
     * on every poll forever.
     *
     * <p>A 410 on one tenant does not abort the cycle: pages consumed before
     * it stay committed, other tenants apply normally, and that pair's cursor
     * fast-forwards past the pruned range.
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
                long next = pullOrFastForward(conn, st, http, baseUrl, c, from, totals);
                if (next > from) {
                    writeChangeOffset(st, c.customerId(), c.tenantId(), next);
                    cursorMoved = true;
                }
            }
            writeRegistryOffset(st, nextSeqOffset);
            boolean changed = cursorMoved || totals.any();
            if (changed) {
                log.info("change stream: {} tenant(s) advanced — ins={} upd={} del={} audit={}",
                    advanced.size(), totals.inserted, totals.updated,
                    totals.deleted, totals.audited);
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
                stageEntries(conn, st, body);
                totals.audited += auditStagedEntries(st, c, from);
                applyStagedEntries(st, c.customerId(), totals);
            });
    }

    /**
     * {@link #pullChanges} with the 410-Gone recovery: pages applied before
     * the 410 stay committed and the cursor fast-forwards to the registry
     * head + 1 (the pruned window is unrecoverable without a snapshot
     * endpoint). Returns the offset to save.
     */
    private static long pullOrFastForward(Connection conn, Statement st, HttpClient http,
                                          String baseUrl, TenantCursor c, long from,
                                          Counts totals) throws SQLException {
        try {
            return pullChanges(conn, st, http, baseUrl, c, from, totals);
        } catch (GoneException gone) {
            long ff = c.lastChangeId() + 1;
            log.warn("change stream: offset {} pruned for customerId={}, tenantId={}"
                + " — fast-forwarding to {} (changes inside the pruned window are lost"
                + " until the entity next changes)", gone.startOffset(),
                c.customerId(), c.tenantId(), ff);
            return ff;
        }
    }

    /**
     * Append the staged page's entries — all entity types, applied or not — to
     * the audit log inside the caller's transaction; returns rows appended.
     * Idempotent: replayed entries hit the change_id NOT EXISTS guard, bounded
     * below by {@code minChangeId} (the pull's start offset — no entry in the
     * page can have a smaller id) so the anti-join prunes the ever-growing
     * audit log instead of scanning all of it.
     *
     * Entries missing a NOT NULL audit column are skipped with a warning —
     * one malformed entry must not abort the transaction (that would pin the
     * cursor and refail on the same page every cycle).
     */
    private static int auditStagedEntries(Statement st, TenantCursor c, long minChangeId)
            throws SQLException {
        long malformed = SqlScripts.queryLong(st, "SELECT count(*) FROM " + ENTRIES_STAGE
            + " WHERE change_id IS NULL OR entity_type IS NULL OR action IS NULL");
        if (malformed > 0) {
            log.warn("change stream: {} malformed /changes entr(y/ies) for customerId={},"
                    + " tenantId={} skipped (missing id/entityType/action) — not auditable"
                    + " or applicable", malformed, c.customerId(), c.tenantId());
        }
        return st.executeUpdate("INSERT INTO " + AUDIT_TABLE
            + " (change_id, customer_id, tenant_id, entity_type, entity_id, action,"
            + " activation_id, txn_id, changed_at, changed_by, sccal_payload, human_payload)"
            + " SELECT change_id, " + c.customerId() + ", " + c.tenantId() + ","
            + " entity_type, entity_id, action, activation_id, txn_id, changed_at,"
            + " changed_by, sccal_payload, human_payload"
            + " FROM " + ENTRIES_STAGE + " e"
            + " WHERE e.change_id IS NOT NULL AND e.entity_type IS NOT NULL"
            + " AND e.action IS NOT NULL"
            + " AND NOT EXISTS (SELECT 1 FROM " + AUDIT_TABLE + " a"
            + " WHERE a.customer_id = " + c.customerId()
            + " AND a.tenant_id = " + c.tenantId()
            + " AND a.change_id >= " + minChangeId
            + " AND a.change_id = e.change_id)");
    }

    /**
     * Shred one raw /changes body into {@link SccalReferenceSync#ENTRIES_STAGE}
     * — the page's single JSON parse; the audit insert and every sync's staging
     * read the parsed rows instead of each re-shredding the body. TRY_CAST
     * throughout: a malformed field becomes NULL (skipped downstream), never an
     * aborted transaction. The raw {@code entry} is kept for the config
     * {@code entry_filter} predicates written against it.
     *
     * <p>The shred is a bare projection on purpose — predicates over
     * {@code entry->>'f'} combined with the unnest column in one query trip a
     * DuckDB planner conversion bug (verified still present on 1.5.4) — so all
     * filtering happens downstream against the materialized columns.
     */
    private static void stageEntries(Connection conn, Statement st, String body)
            throws SQLException {
        st.execute("CREATE OR REPLACE TEMP TABLE " + ENTRIES_STAGE + " (change_id BIGINT,"
            + " entity_type VARCHAR, entity_id BIGINT, action VARCHAR, activation_id BIGINT,"
            + " txn_id BIGINT, changed_at TIMESTAMPTZ, changed_by BIGINT,"
            + " sccal_payload VARCHAR, human_payload VARCHAR, entry JSON)");
        DuckJson.executeWithJson(conn, "INSERT INTO " + ENTRIES_STAGE
            + " SELECT TRY_CAST(entry->>'id' AS BIGINT), entry->>'entityType',"
            + " TRY_CAST(entry->>'entityId' AS BIGINT), entry->>'action',"
            + " TRY_CAST(entry->>'activationId' AS BIGINT),"
            + " TRY_CAST(entry->>'txnId' AS BIGINT),"
            + " TRY_CAST(entry->>'changedAt' AS TIMESTAMPTZ),"
            + " TRY_CAST(entry->>'changedBy' AS BIGINT),"
            + " entry->>'sccal', entry->>'human', entry"
            + " FROM (SELECT unnest(json_extract(?::JSON, '$.entries[*]')) AS entry)", body);
    }

    /**
     * Stage and apply the staged page's entries per fed sync (DELETE / UPDATE /
     * INSERT). Only syncs whose entityType is present in the page are touched —
     * a page usually carries one type, so most syncs cost nothing.
     * {@code customerId} (the pair being pulled) is stamped onto every staged
     * row — it scopes the target table and is not in the entity payload.
     */
    private static void applyStagedEntries(Statement st, long customerId, Counts totals)
            throws SQLException {
        Set<String> present = new HashSet<>();
        try (ResultSet rs = st.executeQuery("SELECT DISTINCT entity_type FROM "
            + ENTRIES_STAGE + " WHERE entity_type IS NOT NULL")) {
            while (rs.next()) {
                present.add(rs.getString(1));
            }
        }
        for (Sync s : SccalReferenceSync.SYNCS) {
            if (!present.contains(s.entityType())) {
                continue;
            }
            st.execute(s.createChangeStageSql());
            int staged = st.executeUpdate(s.stageChangesSql(customerId));
            if (staged > 0) {
                totals.addMerge(s.applyChanges(st));
            }
        }
    }

    // ---- /cursors -------------------------------------------------------------

    /** Page the registry from {@code startOffset} until {@code hasMore} is false. */
    private static CursorPage fetchCursors(Connection conn, HttpClient http, String baseUrl,
                                           long startOffset) throws SQLException {
        List<TenantCursor> all = new ArrayList<>();
        // Every page embeds the same (TTL-cached) gate value — keep the last seen.
        Long[] revision = new Long[1];
        String urlPrefix = baseUrl + SccalReferenceSync.API_PATH + "cursors?";
        long next = pageAll(conn, http, urlPrefix, startOffset, null,
            body -> {
                all.addAll(parseCursors(conn, body));
                Long r = parseGlobalRevision(conn, body);
                if (r != null) {
                    revision[0] = r;
                }
            });
        return new CursorPage(all, next, revision[0]);
    }

    /**
     * The {@code globalCatalog.schemaRevision} gate embedded in a /cursors page
     * body; null when absent or malformed (a server predating the gate).
     */
    private static Long parseGlobalRevision(Connection conn, String body) throws SQLException {
        List<Long> values = DuckJson.queryAll(conn,
            "SELECT TRY_CAST(?::JSON->'globalCatalog'->>'schemaRevision' AS BIGINT)", body,
            rs -> {
                long v = rs.getLong(1);
                return rs.wasNull() ? null : v;
            });
        return values.isEmpty() ? null : values.get(0);
    }

    /** Consumes one page body of an offset-paged endpoint. */
    @FunctionalInterface
    private interface PageHandler {
        void page(String body) throws SQLException;
    }

    /**
     * Drive one offset-paged SCCAL endpoint from {@code startOffset} until
     * {@code hasMore} is false, feeding each body to {@code handler}; returns
     * the offset to save. A page that makes no progress stops the paging
     * defensively. {@code on410}, when non-null, maps the current offset to
     * the exception thrown on HTTP 410; otherwise 410 fails like any non-200.
     */
    private static long pageAll(Connection conn, HttpClient http, String urlPrefix,
                                long startOffset, LongFunction<RuntimeException> on410,
                                PageHandler handler) throws SQLException {
        long offset = startOffset;
        for (int pages = 0; pages < MAX_PAGES_PER_PULL; pages++) {
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
        log.warn("change stream: paging stopped after {} pages without hasMore=false"
            + " ({}) — progress saved, resuming next cycle", MAX_PAGES_PER_PULL, urlPrefix);
        return offset;
    }

    /**
     * Shred one /cursors page with DuckDB (ids arrive as JSON strings), in seq
     * order. TRY_CAST + filter: a malformed registry row is skipped, not a
     * crashed cycle refailing on every poll. The fields materialize in an
     * inner projection before the WHERE — predicates directly over the unnest
     * column trip the DuckDB planner conversion bug.
     */
    private static List<TenantCursor> parseCursors(Connection conn, String body) throws SQLException {
        String sql = "SELECT customer_id, tenant_id, coalesce(last_change_id, 0) FROM ("
            + "SELECT TRY_CAST(e->>'customerId' AS BIGINT) AS customer_id,"
            + " TRY_CAST(e->>'tenantId' AS BIGINT) AS tenant_id,"
            + " TRY_CAST(e->>'lastChangeId' AS BIGINT) AS last_change_id,"
            + " TRY_CAST(e->>'seq' AS BIGINT) AS seq"
            + " FROM (SELECT unnest(json_extract(?::JSON, '$.entries[*]')) AS e))"
            + " WHERE customer_id IS NOT NULL AND tenant_id IS NOT NULL"
            + " ORDER BY seq";
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
        SqlScripts.replaceRows(st, CHANGE_CURSOR_TABLE,
            "customer_id = " + customerId + " AND tenant_id = " + tenantId,
            customerId + ", " + tenantId + ", " + next);
    }

    /** Saved /cursors startOffset; 1 when never saved. */
    private static long readRegistryOffset(Statement st) throws SQLException {
        Long v = SqlScripts.queryNullableLong(st,
            "SELECT max(next_seq_offset) FROM " + REGISTRY_CURSOR_TABLE);
        return (v == null || v < 1) ? 1 : v;
    }

    private static void writeRegistryOffset(Statement st, long next) throws SQLException {
        SqlScripts.replaceRows(st, REGISTRY_CURSOR_TABLE, null, String.valueOf(next));
    }

    // ---- small helpers ----------------------------------------------------------

    /**
     * Collapse duplicate registry rows per pair to the furthest advance — each
     * re-pull would restart from the same saved offset and redo the range.
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
