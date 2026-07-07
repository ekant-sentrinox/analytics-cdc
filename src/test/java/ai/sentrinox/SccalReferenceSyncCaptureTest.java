package ai.sentrinox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end capture test against a real in-memory DuckDB, with the two SCCAL
 * endpoints stubbed. Proves that every stream-fed reference table (USER,
 * WORKSPACE, GROUP) — not just users — is populated from the change stream
 * ({@code /cursors} + {@code /changes}), each row stamped with its owning
 * customer_id; there is no {@code /list} snapshot path. Uses the project's
 * actual V6/V7/V8 schema SQL.
 */
class SccalReferenceSyncCaptureTest {

    private static final long CUST = 66109578638528512L;
    private static final long TEN = 67545582738083840L;

    private Connection conn;
    private Statement st;
    /** Body the /cursors stub returns (any startOffset). */
    private String cursorsJson = "{}";
    /** FIFO of /changes responses: String body (HTTP 200) or Integer status. */
    private final Deque<Object> changesResponses = new ArrayDeque<>();
    /** HTTP status the stub returns for every endpoint; tests flip this to fail a cycle. */
    private int stubStatus = 200;
    private HttpClient http;

    @BeforeEach
    void setUp() throws Exception {
        // Stand the catalog up as a plain in-memory db (no DuckLake/MinIO needed).
        conn = TestSupport.openLake();
        st = conn.createStatement();
        TestSupport.runSqlFile(st, "ollylake/init/V6__reference_tables.sql");
        TestSupport.runSqlFile(st, "ollylake/init/V7__sync_cursor_state.sql");
        TestSupport.runSqlFile(st, "ollylake/init/V8__entity_change_audit.sql");
        http = stubClient();
    }

    @AfterEach
    void tearDown() throws SQLException {
        st.close();
        conn.close();
    }

    @Test
    void fullStreamBootstrapInsertsEveryTable() throws SQLException {
        bootstrap();

        assertEquals(2, count("ollylake.main.workspace"));
        assertEquals(3, count("ollylake.main.\"user\""));
        assertEquals(2, count("ollylake.main.\"group\""));

        // customer_id comes from the poll context (the pulled pair), not the
        // payload, and must be stamped on every captured row.
        assertEquals(CUST, TestSupport.queryLong(st,
            "SELECT customer_id FROM ollylake.main.\"user\" WHERE user_id = 10"));
        assertEquals(CUST, TestSupport.queryLong(st,
            "SELECT customer_id FROM ollylake.main.\"group\" WHERE group_id = 100"));
    }

    @Test
    void capturesGenericPayloadShape() throws SQLException {
        // The stream mixes a generic payload shape ({id, op, type, name/email})
        // with the real one; both must be captured. Here every entry uses the
        // generic shape — key from `id`, user name from `email`, others `name`.
        cursorsJson = cursorsPage(entry(1, 3), 2);
        changesResponses.add(changesPage(4, false,
            change(1, "USER", "CREATE", 1,
                "{\"id\":\"1\",\"op\":\"upsert\",\"type\":\"user\",\"email\":\"alice@acme.com\"}"),
            change(2, "WORKSPACE", "CREATE", 20,
                "{\"id\":\"20\",\"op\":\"upsert\",\"type\":\"workspace\",\"name\":\"ml-platform\"}"),
            change(3, "GROUP", "CREATE", 10,
                "{\"id\":\"10\",\"op\":\"upsert\",\"type\":\"group\",\"name\":\"engineering\"}")));
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals("alice@acme.com", TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.\"user\" WHERE user_id = 1"));
        assertEquals("ml-platform", workspaceName(20));
        assertEquals("engineering", TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.\"group\" WHERE group_id = 10"));
        assertEquals(CUST, TestSupport.queryLong(st,
            "SELECT customer_id FROM ollylake.main.\"user\" WHERE user_id = 1"));
    }

    @Test
    void capturesGenericSoftDeleteViaOp() throws SQLException {
        bootstrap();   // seeds users 10, 20, 30

        // A generic-shape entry whose payload says op:"delete" (no isDeleted) is
        // a tombstone even when the action is not DELETE.
        cursorsJson = cursorsPage(entry(2, 14), 3);
        changesResponses.add(changesPage(15, false,
            change(14, "USER", "UPDATE", 30,
                "{\"id\":\"30\",\"op\":\"delete\",\"type\":\"user\"}")));
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count("ollylake.main.\"user\""));
        assertEquals(0, TestSupport.count(st,
            "(SELECT 1 FROM ollylake.main.\"user\" WHERE user_id = 30)"));
    }

    @Test
    void reRunWhenIdleIsIdempotent() throws SQLException {
        bootstrap();

        // Nothing advanced in the registry: the second cycle is a no-op.
        cursorsJson = "{\"entries\":[],\"nextOffset\":2,\"hasMore\":false}";
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(3, count("ollylake.main.\"user\""));
        assertEquals(2, count("ollylake.main.workspace"));
    }

    @Test
    void capturesAttributeUpdateFromStream() throws SQLException {
        bootstrap();
        assertEquals("ws-2", workspaceName(2));

        cursorsJson = cursorsPage(entry(2, 14), 3);
        changesResponses.add(changesPage(15, false,
            change(14, "WORKSPACE", "UPDATE", 2, "{\"workspaceId\":\"2\",\"workspaceName\":\"ws-renamed\"}")));
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals("ws-renamed", workspaceName(2));
        assertEquals(2, count("ollylake.main.workspace"));         // no spurious insert
    }

    @Test
    void capturesDeleteFromTombstone() throws SQLException {
        bootstrap();
        assertEquals(2, count("ollylake.main.workspace"));

        cursorsJson = cursorsPage(entry(2, 14), 3);
        changesResponses.add(changesPage(15, false,
            change(14, "WORKSPACE", "DELETE", 2, "{\"workspaceId\":\"2\"}")));
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1, count("ollylake.main.workspace"));
    }

    @Test
    void pollCycleSurvivesFailureAndRecoversNextCycle() throws SQLException {
        enqueueBootstrap();

        // A failing cycle (API returns 5xx) must not propagate out of pollCycle...
        stubStatus = 503;
        SccalReferenceSync.pollCycle(conn, st, http, "http://stub");
        assertEquals(0, count("ollylake.main.\"user\""));     // nothing captured

        // ...and once the API is healthy again the next cycle captures normally,
        // proving the connection is still usable after the failed cycle.
        stubStatus = 200;
        SccalReferenceSync.pollCycle(conn, st, http, "http://stub");
        assertEquals(3, count("ollylake.main.\"user\""));
    }

    @Test
    void resetConnectionRestoresAutoCommitAfterAbortedCycle() throws SQLException {
        // Simulate a cycle that died mid-transaction, leaving autoCommit disabled.
        conn.setAutoCommit(false);
        SccalReferenceSync.resetConnection(conn);
        assertTrue(conn.getAutoCommit(),
            "poll must restore autoCommit so the next cycle starts clean");
    }

    // ---- helpers -------------------------------------------------------------

    /**
     * Queue a single change-stream page that creates one row (or more) in every
     * stream-fed reference table (USER, WORKSPACE, GROUP), and point /cursors at
     * the registry head. Change ids 1..7 across the three entity types; the
     * replay ends at offset 8. Payloads use the real ("sccal") field shape.
     */
    private void enqueueBootstrap() {
        cursorsJson = cursorsPage(entry(1, 7), 2);
        changesResponses.add(changesPage(8, false,
            change(1, "WORKSPACE", "CREATE", 1, "{\"workspaceId\":\"1\",\"workspaceName\":\"ws-1\"}"),
            change(2, "WORKSPACE", "CREATE", 2, "{\"workspaceId\":\"2\",\"workspaceName\":\"ws-2\"}"),
            change(3, "USER", "CREATE", 10, "{\"userId\":\"10\",\"userName\":\"a@x.com\"}"),
            change(4, "USER", "CREATE", 20, "{\"userId\":\"20\",\"userName\":\"b@x.com\"}"),
            change(5, "USER", "CREATE", 30, "{\"userId\":\"30\",\"userName\":\"c@x.com\"}"),
            change(6, "GROUP", "CREATE", 100, "{\"groupId\":\"100\",\"groupName\":\"Admins\"}"),
            change(7, "GROUP", "CREATE", 101, "{\"groupId\":\"101\",\"groupName\":\"Devs\"}")));
    }

    /** Enqueue the bootstrap page and run one cycle. */
    private void bootstrap() throws SQLException {
        enqueueBootstrap();
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
    }

    private static String change(long id, String entityType, String action, long entityId,
                                 String sccal) {
        return "{\"id\":\"" + id + "\",\"entityType\":\"" + entityType + "\",\"action\":\""
            + action + "\",\"entityId\":\"" + entityId + "\",\"commitId\":\"1\","
            + "\"changedAt\":\"2026-07-01T00:00:00Z\",\"sccal\":" + sccal + ",\"human\":{}}";
    }

    private static String entry(long seq, long lastChangeId) {
        return "{\"seq\":\"" + seq + "\",\"customerId\":\"" + CUST + "\",\"tenantId\":\"" + TEN
            + "\",\"lastChangeId\":\"" + lastChangeId + "\",\"updatedAt\":\"2026-07-01T00:00:00Z\"}";
    }

    private static String cursorsPage(String entry, long nextOffset) {
        return "{\"entries\":[" + entry + "],\"startOffset\":1,\"limit\":1000,\"nextOffset\":"
            + nextOffset + ",\"hasMore\":false}";
    }

    private static String changesPage(long nextOffset, boolean hasMore, String... entries) {
        return "{\"entries\":[" + String.join(",", entries) + "],\"startOffset\":1,\"limit\":1000,"
            + "\"nextOffset\":" + nextOffset + ",\"hasMore\":" + hasMore + "}";
    }

    private HttpClient stubClient() {
        return TestSupport.httpStub(req -> {
            URI uri = req.uri();
            if (stubStatus != 200) {
                return TestSupport.response(stubStatus, "", uri);
            }
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

    private String workspaceName(long workspaceId) throws SQLException {
        return TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.workspace WHERE workspace_id = " + workspaceId);
    }
}
