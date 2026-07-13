package ai.sentrinox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static ai.sentrinox.TestSupport.changesPage;
import static ai.sentrinox.TestSupport.cursorsPage;
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
    private final TestSupport.SccalStub stub = new TestSupport.SccalStub();

    @BeforeEach
    void setUp() throws Exception {
        conn = TestSupport.openLake();
        st = conn.createStatement();
        TestSupport.runSqlFile(st, "ollylake/init/V6__reference_tables.sql");
        TestSupport.runSqlFile(st, "ollylake/init/V7__sync_cursor_state.sql");
        TestSupport.runSqlFile(st, "ollylake/init/V8__entity_change_audit.sql");

        http = stub.client();
    }

    @AfterEach
    void tearDown() throws SQLException {
        st.close();
        conn.close();
    }

    // ---- bootstrap -------------------------------------------------------------

    @Test
    void bootstrapReplaysStreamAndSeedsCursors() throws SQLException {
        stub.cursorsJson = cursorsPage(entry(5, 42), 6);
        stub.changesResponses.add(changesPage(43, false,
            userChange(41, "CREATE", 10, "a@x.com", false),
            userChange(42, "CREATE", 20, "b@x.com", false)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));
        assertEquals(43, changeOffset());        // where the replay ended
        assertEquals(6, registryOffset());       // the registry page's nextOffset
        assertEquals(2, count(AUDIT_TABLE));     // the replayed entries are audited
    }

    @Test
    void emptyRegistryCapturesNothing() throws SQLException {
        // The registry lists no pairs yet: nothing is known, so nothing is
        // captured and no cursor state is written. First contact with a pair
        // happens when it first appears in the registry.
        stub.cursorsJson = "{\"entries\":[],\"nextOffset\":1,\"hasMore\":false}";

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(0, count(USER_TABLE));
        assertEquals(0, count("ollylake.main.sccal_change_cursor"));
        assertEquals(0, count("ollylake.main.sccal_registry_cursor"));
    }

    // ---- steady state -----------------------------------------------------------

    @Test
    void idleCursorsSkipCaptureEntirely() throws SQLException {
        bootstrap();

        // The registry reports no advance: the stream-fed tables must not be
        // touched and the cursor must stay put.
        stub.cursorsJson = "{\"entries\":[],\"nextOffset\":6,\"hasMore\":false}";
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));
        assertTrue(userExists(10));
        assertEquals(43, changeOffset());        // cursor untouched
    }

    @Test
    void streamAppliesCreateUpdateDeleteAndIgnoresActivation() throws SQLException {
        bootstrap();

        stub.cursorsJson = cursorsPage(entry(7, 46), 8);
        stub.changesResponses.add(changesPage(47, false,
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
            "SELECT entity_type, action, activation_id, txn_id, changed_by,"
                + " sccal_payload::JSON->>'userName', human_payload::JSON->>'email'"
                + " FROM " + AUDIT_TABLE + " WHERE change_id = 44")) {
            assertTrue(rs.next());
            assertEquals("USER", rs.getString(1));
            assertEquals("UPDATE", rs.getString(2));
            assertEquals(77, rs.getLong(3));
            assertEquals(88, rs.getLong(4));
            assertEquals(99, rs.getLong(5));
            assertEquals("renamed@x.com", rs.getString(6));
            assertEquals("renamed@x.com", rs.getString(7));
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

        stub.cursorsJson = cursorsPage(entry(7, 44), 8);
        stub.changesResponses.add(changesPage(44, true,
            userChange(43, "CREATE", 30, "first@x.com", false)));
        stub.changesResponses.add(changesPage(45, false,
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
        stub.cursorsJson = cursorsPage(entry(7, 43), 8);
        stub.changesResponses.add(changesPage(44, false,
            userChange(43, "UPDATE", 20, "b@x.com", true)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1, count(USER_TABLE));
        assertTrue(!userExists(20));
        assertEquals(44, changeOffset());
    }

    @Test
    void createThenDeleteInOnePageNetsToNoRow() throws SQLException {
        bootstrap();

        stub.cursorsJson = cursorsPage(entry(7, 44), 8);
        stub.changesResponses.add(changesPage(45, false,
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

        stub.cursorsJson = cursorsPage(entry(7, 43), 8);
        stub.changesResponses.add(page);
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        // Same registry row again (e.g. crash before this consumer's state was
        // read back) → the same page replays; the merge must converge.
        st.execute("DELETE FROM ollylake.main.sccal_change_cursor");
        st.execute("INSERT INTO ollylake.main.sccal_change_cursor VALUES (" + CUST + ", " + TEN + ", 43)");
        st.execute("DELETE FROM ollylake.main.sccal_registry_cursor");
        stub.changesResponses.add(page);
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
        stub.cursorsJson = cursorsPage(entry(9, 100), 10);
        stub.changesResponses.add(410);

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));      // pruned changes not recovered
        assertTrue(userExists(10));
        assertTrue(userExists(20));
        assertEquals(101, changeOffset());       // fast-forwarded to lastChangeId + 1
        assertEquals(10, registryOffset());
    }

    @Test
    void goneFastForwardsOnlyTheAffectedPair() throws SQLException {
        // Two pairs in the registry; both bootstrap in one run by replaying
        // their streams from the start.
        stub.cursorsJson = cursorsPage(
            entry(5, CUST, TEN, 42) + "," + entry(6, 3, 4, 50), 7);
        stub.changesResponses.add(changesPage(43, false,
            userChange(41, "CREATE", 10, "a@x.com", false)));   // (CUST, TEN)
        stub.changesResponses.add(changesPage(51, false,
            userChange(50, "CREATE", 99, "z@x.com", false)));   // (3, 4)
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals(43, changeOffsetFor(CUST, TEN));
        assertEquals(51, changeOffsetFor(3, 4));

        // (CUST, TEN) advances but its saved offset was pruned: only that pair's
        // cursor fast-forwards — the healthy pair keeps its cursor.
        stub.cursorsJson = cursorsPage(entry(8, CUST, TEN, 60), 9);
        stub.changesResponses.add(410);
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
        stub.cursorsJson = cursorsPage(entry(7, 46), 8);
        stub.changesResponses.add(changesPage(44, true,
            userChange(43, "CREATE", 30, "c@x.com", false)));
        stub.changesResponses.add(410);

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
        stub.cursorsJson = "{\"entries\":[" + entries + "],\"startOffset\":1,\"limit\":1000,"
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
        stub.cursorsJson = cursorsPage(entry(7, 44), 8);
        stub.changesResponses.add(changesPage(45, false,
            "{\"id\":\"43\",\"entityType\":\"USER\",\"action\":\"CREATE\",\"entityId\":\"30\","
                + "\"activationId\":\"77\",\"txnId\":\"88\",\"changedAt\":\"2026-07-01T00:00:00Z\","
                + "\"sccal\":{\"userId\":\"30\"},\"human\":{}}",
            userChange(44, "UPDATE", 20, "renamed@x.com", false)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));              // poison row not inserted
        assertTrue(!userExists(30));
        assertEquals("renamed@x.com", userName(20));     // rest of the page applied
        assertEquals(45, changeOffset());                // cursor moved past the poison
        assertEquals(2, count(AUDIT_TABLE));             // both audited
    }

    @Test
    void malformedValuePoisonEntryIsSkippedNotWedged() throws SQLException {
        bootstrap();

        // Entry 43's userId is present but non-numeric: TRY_CAST turns it into
        // a NULL key that the NOT NULL guards skip. A hard cast would abort
        // the transaction with the cursor unmoved and refail on the same page
        // every cycle.
        stub.cursorsJson = cursorsPage(entry(7, 44), 8);
        stub.changesResponses.add(changesPage(45, false,
            TestSupport.changeEntry(43, "USER", "CREATE", 30,
                "{\"userId\":\"not-a-number\",\"userName\":\"x@x.com\"}", "{}"),
            userChange(44, "UPDATE", 20, "renamed@x.com", false)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count(USER_TABLE));              // poison row not inserted
        assertEquals("renamed@x.com", userName(20));     // rest of the page applied
        assertEquals(45, changeOffset());                // cursor moved past the poison
        assertEquals(2, count(AUDIT_TABLE));             // both audited
    }

    // ---- first-contact bootstrap / failure isolation ----------------------------

    @Test
    void newPairInRegistryBootstrapsWithoutTouchingExistingCursors() throws SQLException {
        bootstrap();                                    // (CUST, TEN) at 43, registry at 6

        // A never-seen pair appears in the registry: it has no saved cursor, so
        // its stream replays from the start; the healthy pair's cursor stays put.
        stub.cursorsJson = cursorsPage(entry(7, 3, 4, 50), 8);
        stub.changesResponses.add(changesPage(51, false,
            userChange(50, "CREATE", 99, "z@x.com", false)));

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(43, changeOffsetFor(CUST, TEN));   // healthy pair keeps its cursor
        assertEquals(51, changeOffsetFor(3, 4));        // new pair: replayed from offset 1
        assertTrue(userExists(99));
        assertEquals(8, registryOffset());
    }

    @Test
    void alreadyConsumedAdvanceRollsBackInsteadOfCommitting() throws SQLException {
        bootstrap();                                    // cursor 43, registry 6

        // The registry reports an advance our cursor already covers: nothing to
        // pull, so the cycle must roll back rather than write a metadata-only
        // DuckLake snapshot.
        stub.cursorsJson = cursorsPage(entry(7, 40), 8);
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(43, changeOffset());
        assertEquals(6, registryOffset());              // rolled back, not 8
    }

    @Test
    void streamFailurePropagatesAndRollsBack() throws SQLException {
        bootstrap();

        // Mid-deploy environment: the V8 audit table is missing, so the change
        // stream fails on its INSERT. With no snapshot phase the failure
        // propagates and the whole stream transaction rolls back.
        st.execute("DROP TABLE " + AUDIT_TABLE);
        stub.cursorsJson = cursorsPage(entry(7, 46), 8);
        stub.changesResponses.add(changesPage(47, false,
            userChange(43, "CREATE", 30, "c@x.com", false)));

        assertThrows(SQLException.class,
            () -> SccalReferenceSync.runOnce(conn, st, http, "http://stub"));

        // DuckDB's JDBC driver closes a Statement whose execute threw; the poll
        // loop re-bootstraps in that case (usable() check) — mirror it here.
        st = conn.createStatement();
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
        stub.cursorsJson = cursorsPage(entry(5, 42), 6);
        stub.changesResponses.add(changesPage(43, false,
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
        return TestSupport.cursorEntry(seq, customerId, tenantId, lastChangeId);
    }

    private static String userChange(long id, String action, long userId, String userName,
                                     boolean isDeleted) {
        return TestSupport.changeEntry(id, "USER", action, userId,
            "{\"userId\":\"" + userId + "\",\"userName\":\"" + userName
                + "\",\"isDeleted\":" + isDeleted + "}",
            "{\"userId\":\"" + userId + "\",\"email\":\"" + userName
                + "\",\"status\":\"ACTIVE\"}");
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
