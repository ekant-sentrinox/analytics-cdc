package ai.sentrinox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for the change-stream capture path ({@link ChangeStreamSync})
 * against a real in-memory DuckDB, with the two SCCAL endpoints stubbed:
 * {@code /cursors} answers from configurable state; {@code /changes} answers
 * from a FIFO queue (so multi-page and 410 sequences can be scripted). There is
 * no {@code /list} endpoint — every reference table is fed from the stream.
 */
class ChangeStreamCaptureTest {

    private static final long CUST = 1L;
    private static final long TEN = 2L;
    private static final String USER_TABLE = "ollylake.main.\"user\"";
    private static final String AUDIT_TABLE = "ollylake.main.entity_change_audit";

    private Connection conn;
    private Statement st;
    private HttpClient http;

    /** Body the /cursors stub returns (any startOffset). */
    private String cursorsJson = "{}";
    /** FIFO of /changes responses: String body (HTTP 200) or Integer status. */
    private final Deque<Object> changesResponses = new ArrayDeque<>();

    @BeforeEach
    void setUp() throws Exception {
        conn = TestSupport.openLake();
        st = conn.createStatement();
        TestSupport.runSqlFile(st, "ollylake/init/V6__reference_tables.sql");
        TestSupport.runSqlFile(st, "ollylake/init/V1__create_customer_tenant_ids_table.sql");
        TestSupport.runSqlFile(st, "ollylake/init/V7__sync_cursor_state.sql");
        TestSupport.runSqlFile(st, "ollylake/init/V8__entity_change_audit.sql");
        // Replace the V1 seed with a pair whose ids the stub JSON uses.
        st.execute("DELETE FROM ollylake.main.customer_tenant_reference");
        st.execute("INSERT INTO ollylake.main.customer_tenant_reference VALUES (" + CUST + ", " + TEN + ")");

        http = stubClient();
    }

    @AfterEach
    void tearDown() throws SQLException {
        st.close();
        conn.close();
    }

    // ---- bootstrap -------------------------------------------------------------

    @Test
    void bootstrapReplaysStreamAndSeedsCursors() throws SQLException {
        cursorsJson = cursorsPage(entry(5, 42), 6);
        changesResponses.add(changesPage(43, false,
            userChange(41, "CREATE", 10, "a@x.com", false),
            userChange(42, "CREATE", 20, "b@x.com", false)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));
        assertEquals(43, changeOffset());        // where the replay ended
        assertEquals(6, registryOffset());       // full bootstrap → registry head
        assertEquals(2, count(AUDIT_TABLE));     // the replayed entries are audited
    }

    @Test
    void bootstrapWithEmptyStreamSeedsOffsetOne() throws SQLException {
        // Registry lists the pair with no changes yet; /changes returns an empty
        // page. The bootstrap seeds the cursor at the stream start (offset 1) and
        // captures nothing — the next cycle picks up any later changes.
        cursorsJson = "{\"entries\":[],\"nextOffset\":1,\"hasMore\":false}";

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(0, count(USER_TABLE));
        assertEquals(1, changeOffset());
        assertEquals(1, registryOffset());
    }

    // ---- steady state -----------------------------------------------------------

    @Test
    void idleCursorsSkipCaptureEntirely() throws SQLException {
        bootstrap();

        // The registry reports no advance: the stream-fed tables must not be
        // touched and the cursor must stay put.
        cursorsJson = "{\"entries\":[],\"nextOffset\":6,\"hasMore\":false}";
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));
        assertTrue(userExists(10));
        assertEquals(43, changeOffset());        // cursor untouched
    }

    @Test
    void streamAppliesCreateUpdateDeleteAndIgnoresActivation() throws SQLException {
        bootstrap();

        cursorsJson = cursorsPage(entry(7, 46), 8);
        changesResponses.add(changesPage(47, false,
            userChange(43, "CREATE", 30, "c@x.com", false),
            userChange(44, "UPDATE", 20, "renamed@x.com", false),
            userChange(45, "DELETE", 10, "a@x.com", true),
            "{\"id\":\"46\",\"entityType\":\"ACTIVATION\",\"action\":\"ACTIVATE\","
                + "\"sccal\":{\"modifiedTypes\":[\"USER\"],\"isForced\":false}}"));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));      // +30, -10
        assertTrue(userExists(30));
        assertEquals("renamed@x.com", userName(20));
        assertEquals(47, changeOffset());        // the page's nextOffset
        assertEquals(8, registryOffset());

        // Every entry landed in the audit log — including the ACTIVATION one
        // that feeds no reference table — with both payload views.
        assertEquals(4, count(AUDIT_TABLE));
        try (ResultSet rs = st.executeQuery(
            "SELECT entity_type, action, commit_id,"
                + " sccal_payload::JSON->>'userName', human_payload::JSON->>'email'"
                + " FROM " + AUDIT_TABLE + " WHERE change_id = 44")) {
            assertTrue(rs.next());
            assertEquals("USER", rs.getString(1));
            assertEquals("UPDATE", rs.getString(2));
            assertEquals(77, rs.getLong(3));
            assertEquals("renamed@x.com", rs.getString(4));
            assertEquals("renamed@x.com", rs.getString(5));
        }
        try (ResultSet rs = st.executeQuery(
            "SELECT action FROM " + AUDIT_TABLE + " WHERE change_id = 46")) {
            assertTrue(rs.next());
            assertEquals("ACTIVATE", rs.getString(1));
        }
    }

    @Test
    void streamPagesUntilHasMoreIsFalse() throws SQLException {
        bootstrap();

        cursorsJson = cursorsPage(entry(7, 44), 8);
        changesResponses.add(changesPage(44, true,
            userChange(43, "CREATE", 30, "first@x.com", false)));
        changesResponses.add(changesPage(45, false,
            userChange(44, "UPDATE", 30, "second@x.com", false)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals("second@x.com", userName(30));
        assertEquals(45, changeOffset());
    }

    @Test
    void softDeleteViaUpdateActionRemovesTheRow() throws SQLException {
        bootstrap();

        // A deactivation modelled as an UPDATE whose payload says isDeleted:true
        // must remove the row.
        cursorsJson = cursorsPage(entry(7, 43), 8);
        changesResponses.add(changesPage(44, false,
            userChange(43, "UPDATE", 20, "b@x.com", true)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1, count(USER_TABLE));
        assertTrue(!userExists(20));
        assertEquals(44, changeOffset());
    }

    @Test
    void createThenDeleteInOnePageNetsToNoRow() throws SQLException {
        bootstrap();

        cursorsJson = cursorsPage(entry(7, 44), 8);
        changesResponses.add(changesPage(45, false,
            userChange(43, "CREATE", 30, "ghost@x.com", false),
            userChange(44, "DELETE", 30, "ghost@x.com", true)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));      // 10 and 20 only
        assertTrue(!userExists(30));
    }

    @Test
    void replayedPageIsIdempotent() throws SQLException {
        bootstrap();

        String page = changesPage(44, false,
            userChange(43, "CREATE", 30, "c@x.com", false));

        cursorsJson = cursorsPage(entry(7, 43), 8);
        changesResponses.add(page);
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        // Same registry row again (e.g. crash before this consumer's state was
        // read back) → the same page replays; the merge must converge.
        st.execute("DELETE FROM ollylake.main.sccal_change_cursor");
        st.execute("INSERT INTO ollylake.main.sccal_change_cursor VALUES (" + CUST + ", " + TEN + ", 43)");
        st.execute("DELETE FROM ollylake.main.sccal_registry_cursor");
        changesResponses.add(page);
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(3, count(USER_TABLE));
        assertEquals("c@x.com", userName(30));
        // The replayed page must not duplicate its audit rows either.
        assertEquals(1, count(AUDIT_TABLE));
    }

    // ---- 410 Gone ----------------------------------------------------------------

    @Test
    void goneFastForwardsPastPrunedRange() throws SQLException {
        bootstrap();

        // The stream advanced far past our cursor and the low partitions were
        // pruned: /changes answers 410. With no snapshot fallback the pruned
        // changes are lost; the cursor fast-forwards to the registry head so the
        // stream keeps flowing.
        cursorsJson = cursorsPage(entry(9, 100), 10);
        changesResponses.add(410);

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));      // pruned changes not recovered
        assertTrue(userExists(10));
        assertTrue(userExists(20));
        assertEquals(101, changeOffset());       // fast-forwarded to lastChangeId + 1
        assertEquals(10, registryOffset());
    }

    @Test
    void goneFastForwardsOnlyTheAffectedPair() throws SQLException {
        // Two tracked pairs; both bootstrap in one run by replaying their streams.
        st.execute("INSERT INTO ollylake.main.customer_tenant_reference VALUES (3, 4)");
        cursorsJson = cursorsPage(
            entry(5, CUST, TEN, 42) + "," + entry(6, 3, 4, 50), 7);
        changesResponses.add(changesPage(43, false,
            userChange(41, "CREATE", 10, "a@x.com", false)));   // (CUST, TEN)
        changesResponses.add(changesPage(51, false,
            userChange(50, "CREATE", 99, "z@x.com", false)));   // (3, 4)
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals(43, changeOffsetFor(CUST, TEN));
        assertEquals(51, changeOffsetFor(3, 4));

        // (CUST, TEN) advances but its saved offset was pruned: only that pair's
        // cursor fast-forwards — the healthy pair keeps its cursor.
        cursorsJson = cursorsPage(entry(8, CUST, TEN, 60), 9);
        changesResponses.add(410);
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(61, changeOffsetFor(CUST, TEN));  // fast-forwarded to 60 + 1
        assertEquals(51, changeOffsetFor(3, 4));       // untouched
        assertEquals(9, registryOffset());             // steady state advances the registry
    }

    @Test
    void goneOnLaterPageKeepsPagesConsumedBeforeIt() throws SQLException {
        bootstrap();                                    // cursor 43, registry 6

        // Page 1 (offset 43) succeeds and is applied + audited; page 2 answers
        // 410. The pre-410 page must survive — cursor advanced past it and
        // committed, audit rows kept — with the fast-forward covering the rest.
        cursorsJson = cursorsPage(entry(7, 46), 8);
        changesResponses.add(changesPage(44, true,
            userChange(43, "CREATE", 30, "c@x.com", false)));
        changesResponses.add(410);

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1, count(AUDIT_TABLE));     // page-1 audit kept
        assertEquals(3, count(USER_TABLE));
        assertTrue(userExists(30));
        assertEquals(47, changeOffset());        // fast-forwarded to 46 + 1
        assertEquals(8, registryOffset());
    }

    @Test
    void fullPageOfAlreadyConsumedRowsAdvancesRegistryOffset() throws SQLException {
        bootstrap();                                    // cursor 43, registry 6

        // A full page of registry rows for a tracked pair, all below its saved
        // cursor: nothing to pull, but rolling back would re-page this backlog on
        // every poll forever — the full-page escape must pay one commit to skip it.
        StringBuilder entries = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            if (i > 0) {
                entries.append(',');
            }
            entries.append(entry(100 + i, 40));
        }
        cursorsJson = "{\"entries\":[" + entries + "],\"startOffset\":1,\"limit\":1000,"
            + "\"nextOffset\":1100,\"hasMore\":false}";

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1100, registryOffset());           // escaped the wedge
        assertEquals(43, changeOffset());               // cursor untouched
        assertEquals(2, count(USER_TABLE));             // nothing captured
    }

    @Test
    void poisonEntryIsSkippedNotWedged() throws SQLException {
        bootstrap();

        // Entry 43's payload omits userName (NOT NULL in V6): applying it would
        // abort the transaction and refail on the same page every cycle. It must
        // be skipped — kept in the audit log — with the rest of the page applied
        // and the cursor advanced past it.
        cursorsJson = cursorsPage(entry(7, 44), 8);
        changesResponses.add(changesPage(45, false,
            "{\"id\":\"43\",\"entityType\":\"USER\",\"action\":\"CREATE\",\"entityId\":\"30\","
                + "\"commitId\":\"77\",\"changedAt\":\"2026-07-01T00:00:00Z\","
                + "\"sccal\":{\"userId\":\"30\"},\"human\":{}}",
            userChange(44, "UPDATE", 20, "renamed@x.com", false)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));              // poison row not inserted
        assertTrue(!userExists(30));
        assertEquals("renamed@x.com", userName(20));     // rest of the page applied
        assertEquals(45, changeOffset());                // cursor moved past the poison
        assertEquals(2, count(AUDIT_TABLE));             // both audited
    }

    // ---- scoped bootstrap / failure isolation ----------------------------------

    @Test
    void newPairBootstrapsWithoutWipingExistingCursors() throws SQLException {
        bootstrap();                                    // (CUST, TEN) at 43, registry at 6

        st.execute("INSERT INTO ollylake.main.customer_tenant_reference VALUES (3, 4)");
        // (3, 4) is missing → partial bootstrap; no /changes queued → empty replay.
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(43, changeOffsetFor(CUST, TEN));   // healthy pair keeps its cursor
        assertEquals(1, changeOffsetFor(3, 4));         // new pair: empty replay → offset 1
        assertEquals(6, registryOffset());              // partial bootstrap leaves the registry offset
    }

    @Test
    void alreadyConsumedAdvanceRollsBackInsteadOfCommitting() throws SQLException {
        bootstrap();                                    // cursor 43, registry 6

        // The registry reports an advance our cursor already covers: nothing to
        // pull, so the cycle must roll back rather than write a metadata-only
        // DuckLake snapshot.
        cursorsJson = cursorsPage(entry(7, 40), 8);
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(43, changeOffset());
        assertEquals(6, registryOffset());              // rolled back, not 8
    }

    @Test
    void largeOutOfScopeBacklogAdvancesRegistryOffsetWhenIdle() throws SQLException {
        bootstrap();                                    // registry at 6

        // A full page of registry rows, all for an untracked pair: idle, but
        // leaving the offset would re-page this backlog on every future poll.
        StringBuilder entries = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            if (i > 0) {
                entries.append(',');
            }
            entries.append(entry(100 + i, 9, 9, 5));
        }
        cursorsJson = "{\"entries\":[" + entries + "],\"startOffset\":1,\"limit\":1000,"
            + "\"nextOffset\":1100,\"hasMore\":false}";

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1100, registryOffset());           // paid one commit to skip the backlog
        assertEquals(43, changeOffset());               // in-scope cursor untouched
        assertEquals(2, count(USER_TABLE));             // nothing captured
    }

    @Test
    void streamFailurePropagatesAndRollsBack() throws SQLException {
        bootstrap();

        // Mid-deploy environment: the V8 audit table is missing, so the change
        // stream fails on its INSERT. With no snapshot phase the failure
        // propagates and the whole stream transaction rolls back.
        st.execute("DROP TABLE " + AUDIT_TABLE);
        cursorsJson = cursorsPage(entry(7, 46), 8);
        changesResponses.add(changesPage(47, false,
            userChange(43, "CREATE", 30, "c@x.com", false)));

        assertThrows(SQLException.class,
            () -> SccalReferenceSync.runOnce(conn, st, http, "http://stub"));

        assertEquals(43, changeOffset());                   // stream txn rolled back
        assertEquals(2, count(USER_TABLE));                 // stream data rolled back too
    }

    // ---- helpers -------------------------------------------------------------

    /**
     * First run: replays the change stream to seed users 10+20 and the cursor at
     * change 42's page (offset 43). The replayed entries are audited during
     * bootstrap; the audit log is cleared afterwards so each steady-state test
     * counts only its own audit rows.
     */
    private void bootstrap() throws SQLException {
        cursorsJson = cursorsPage(entry(5, 42), 6);
        changesResponses.add(changesPage(43, false,
            userChange(41, "CREATE", 10, "a@x.com", false),
            userChange(42, "CREATE", 20, "b@x.com", false)));
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals(2, count(USER_TABLE));
        st.execute("DELETE FROM " + AUDIT_TABLE);
    }

    private static String entry(long seq, long lastChangeId) {
        return entry(seq, CUST, TEN, lastChangeId);
    }

    private static String entry(long seq, long customerId, long tenantId, long lastChangeId) {
        return "{\"seq\":\"" + seq + "\",\"customerId\":\"" + customerId + "\",\"tenantId\":\""
            + tenantId + "\",\"lastChangeId\":\"" + lastChangeId + "\",\"lastActivationId\":\"9\","
            + "\"updatedAt\":\"2026-07-01T00:00:00Z\"}";
    }

    private static String cursorsPage(String entry, long nextOffset) {
        return "{\"entries\":[" + entry + "],\"startOffset\":1,\"limit\":1000,\"nextOffset\":"
            + nextOffset + ",\"hasMore\":false}";
    }

    private static String changesPage(long nextOffset, boolean hasMore, String... entries) {
        return "{\"entries\":[" + String.join(",", entries) + "],\"startOffset\":1,\"limit\":1000,"
            + "\"nextOffset\":" + nextOffset + ",\"hasMore\":" + hasMore + "}";
    }

    private static String userChange(long id, String action, long userId, String userName,
                                     boolean isDeleted) {
        return "{\"id\":\"" + id + "\",\"entityType\":\"USER\",\"action\":\"" + action
            + "\",\"entityId\":\"" + userId + "\",\"commitId\":\"77\","
            + "\"changedAt\":\"2026-07-01T00:00:00Z\","
            + "\"sccal\":{\"userId\":\"" + userId + "\",\"userName\":\"" + userName
            + "\",\"isDeleted\":" + isDeleted + "},"
            + "\"human\":{\"userId\":\"" + userId + "\",\"email\":\"" + userName
            + "\",\"status\":\"ACTIVE\"}}";
    }

    private HttpClient stubClient() {
        return TestSupport.httpStub(req -> {
            URI uri = req.uri();
            String path = uri.getPath();
            if (path.endsWith("/cursors")) {
                return TestSupport.response(200, cursorsJson, uri);
            }
            if (path.endsWith("/changes")) {
                Object next = changesResponses.isEmpty() ? "{}" : changesResponses.poll();
                return next instanceof Integer status
                    ? TestSupport.response(status, "", uri)
                    : TestSupport.response(200, (String) next, uri);
            }
            throw new AssertionError("unexpected endpoint (only /cursors and /changes exist): " + path);
        });
    }

    private long count(String table) throws SQLException {
        return TestSupport.count(st, table);
    }

    private boolean userExists(long userId) throws SQLException {
        return TestSupport.exists(st, "SELECT 1 FROM " + USER_TABLE + " WHERE user_id = " + userId);
    }

    private String userName(long userId) throws SQLException {
        return TestSupport.queryString(st,
            "SELECT name FROM " + USER_TABLE + " WHERE user_id = " + userId);
    }

    private long changeOffset() throws SQLException {
        return changeOffsetFor(CUST, TEN);
    }

    private long changeOffsetFor(long customerId, long tenantId) throws SQLException {
        return TestSupport.queryLong(st,
            "SELECT next_change_offset FROM ollylake.main.sccal_change_cursor "
                + "WHERE customer_id = " + customerId + " AND tenant_id = " + tenantId);
    }

    private long registryOffset() throws SQLException {
        return TestSupport.queryLong(st,
            "SELECT next_seq_offset FROM ollylake.main.sccal_registry_cursor");
    }
}
