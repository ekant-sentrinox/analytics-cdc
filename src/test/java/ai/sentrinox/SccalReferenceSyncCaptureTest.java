package ai.sentrinox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static ai.sentrinox.TestSupport.changesPage;
import static ai.sentrinox.TestSupport.cursorsPage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end capture test against a real in-memory DuckDB, with the two SCCAL
 * endpoints stubbed. Proves that every stream-fed reference table (USER,
 * WORKSPACE, group via the USERGROUP entityType) — not just users — is
 * populated from the change stream
 * ({@code /cursors} + {@code /changes}), each row stamped with its owning
 * customer_id; the stream-fed tables have no {@code /list} snapshot path
 * (the catalogue {@code /list} pull is a separate tier — see
 * {@link CatalogueCaptureTest}). Uses the project's actual schema SQL.
 */
class SccalReferenceSyncCaptureTest {

    private static final long CUST = 66109578638528512L;
    private static final long TEN = 67545582738083840L;

    private Connection conn;
    private Statement st;
    private HttpClient http;
    private final TestSupport.SccalStub stub = new TestSupport.SccalStub();

    @BeforeEach
    void setUp() throws Exception {
        // Stand the catalog up as a plain in-memory db (no DuckLake/MinIO needed).
        conn = TestSupport.openLake();
        st = conn.createStatement();
        TestSupport.runInitMigrations(st);
        http = stub.client();
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
        stub.cursorsJson = cursorsPage(entry(1, 3), 2);
        stub.changesResponses.add(changesPage(4, false,
            change(1, "USER", "CREATE", 1,
                "{\"id\":\"1\",\"op\":\"upsert\",\"type\":\"user\",\"email\":\"alice@acme.com\"}"),
            change(2, "WORKSPACE", "CREATE", 20,
                "{\"id\":\"20\",\"op\":\"upsert\",\"type\":\"workspace\",\"name\":\"ml-platform\"}"),
            change(3, "USERGROUP", "CREATE", 10,
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
        // a tombstone even when the action is not DELETE: the row is kept and
        // flagged is_deleted.
        stub.cursorsJson = cursorsPage(entry(2, 14), 3);
        stub.changesResponses.add(changesPage(15, false,
            change(14, "USER", "UPDATE", 30,
                "{\"id\":\"30\",\"op\":\"delete\",\"type\":\"user\"}")));
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(3, count("ollylake.main.\"user\""));
        assertTrue(TestSupport.exists(st,
            "SELECT 1 FROM ollylake.main.\"user\" WHERE user_id = 30 AND is_deleted"));
    }

    @Test
    void reRunWhenIdleIsIdempotent() throws SQLException {
        bootstrap();

        // Nothing advanced in the registry: the second cycle is a no-op.
        stub.cursorsJson = "{\"entries\":[],\"nextOffset\":2,\"hasMore\":false}";
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(3, count("ollylake.main.\"user\""));
        assertEquals(2, count("ollylake.main.workspace"));
    }

    @Test
    void capturesAttributeUpdateFromStream() throws SQLException {
        bootstrap();
        assertEquals("ws-2", workspaceName(2));

        stub.cursorsJson = cursorsPage(entry(2, 14), 3);
        stub.changesResponses.add(changesPage(15, false,
            change(14, "WORKSPACE", "UPDATE", 2, "{\"workspaceId\":\"2\",\"workspaceName\":\"ws-renamed\"}")));
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals("ws-renamed", workspaceName(2));
        assertEquals(2, count("ollylake.main.workspace"));         // no spurious insert
    }

    @Test
    void capturesDeleteFromTombstone() throws SQLException {
        bootstrap();
        assertEquals(2, count("ollylake.main.workspace"));

        stub.cursorsJson = cursorsPage(entry(2, 14), 3);
        stub.changesResponses.add(changesPage(15, false,
            change(14, "WORKSPACE", "DELETE", 2, "{\"workspaceId\":\"2\"}")));
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        // Soft-delete: the row is kept and flagged, not removed.
        assertEquals(2, count("ollylake.main.workspace"));
        assertTrue(TestSupport.exists(st,
            "SELECT 1 FROM ollylake.main.workspace WHERE workspace_id = 2 AND is_deleted"));
    }

    @Test
    void splitsRuleByRuleTypeIntoLlmOrMcpAccessRule() throws SQLException {
        // RULE fans out by the payload's ruleType, not entityType: 0/1 is an LLM
        // access rule, 2/3 an MCP one (LlmPolicyService/McpPolicyService both
        // publish plain RULE — there is no separate entityType to key off).
        stub.cursorsJson = cursorsPage(entry(1, 4), 2);
        stub.changesResponses.add(changesPage(5, false,
            change(1, "RULE", "CREATE", 500,
                "{\"ruleId\":\"500\",\"name\":\"Curl Allow Gemini\",\"action\":0,"
                    + "\"priority\":1,\"ruleType\":0,\"isDeleted\":false}"),
            change(2, "RULE", "CREATE", 501,
                "{\"ruleId\":\"501\",\"name\":\"Allow Claude\",\"action\":0,"
                    + "\"priority\":2,\"ruleType\":1,\"isDeleted\":false}"),
            change(3, "RULE", "CREATE", 502,
                "{\"ruleId\":\"502\",\"name\":\"Allow Filesystem MCP\",\"action\":0,"
                    + "\"priority\":1,\"ruleType\":2,\"isDeleted\":false}"),
            change(4, "RULE", "CREATE", 503,
                "{\"ruleId\":\"503\",\"name\":\"Allow Search MCP\",\"action\":0,"
                    + "\"priority\":2,\"ruleType\":3,\"isDeleted\":false}")));
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count("ollylake.main.llm_access_rule"));
        assertEquals(2, count("ollylake.main.mcp_access_rule"));
        assertEquals("Curl Allow Gemini", TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.llm_access_rule WHERE llm_access_rule_id = 500"));
        assertEquals("Allow Claude", TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.llm_access_rule WHERE llm_access_rule_id = 501"));
        assertEquals("Allow Filesystem MCP", TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.mcp_access_rule WHERE mcp_access_rule_id = 502"));
        assertEquals("Allow Search MCP", TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.mcp_access_rule WHERE mcp_access_rule_id = 503"));
        assertEquals(CUST, TestSupport.queryLong(st,
            "SELECT customer_id FROM ollylake.main.llm_access_rule WHERE llm_access_rule_id = 500"));
    }

    @Test
    void pollCycleSurvivesFailureAndRecoversNextCycle() throws SQLException {
        enqueueBootstrap();

        // A failing cycle (API returns 5xx) must not propagate out of pollCycle...
        stub.status = 503;
        SccalReferenceSync.pollCycle(conn, st, http, "http://stub");
        assertEquals(0, count("ollylake.main.\"user\""));     // nothing captured

        // ...and once the API is healthy again the next cycle captures normally,
        // proving the connection is still usable after the failed cycle.
        stub.status = 200;
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
     * stream-fed reference table (USER, WORKSPACE, group via USERGROUP), and
     * point /cursors at the registry head. Change ids 1..7 across the three
     * entity types; the replay ends at offset 8. Payloads use the real
     * ("sccal") field shape.
     */
    private void enqueueBootstrap() {
        stub.cursorsJson = cursorsPage(entry(1, 7), 2);
        stub.changesResponses.add(changesPage(8, false,
            change(1, "WORKSPACE", "CREATE", 1, "{\"workspaceId\":\"1\",\"workspaceName\":\"ws-1\"}"),
            change(2, "WORKSPACE", "CREATE", 2, "{\"workspaceId\":\"2\",\"workspaceName\":\"ws-2\"}"),
            change(3, "USER", "CREATE", 10, "{\"userId\":\"10\",\"userName\":\"a@x.com\"}"),
            change(4, "USER", "CREATE", 20, "{\"userId\":\"20\",\"userName\":\"b@x.com\"}"),
            change(5, "USER", "CREATE", 30, "{\"userId\":\"30\",\"userName\":\"c@x.com\"}"),
            change(6, "USERGROUP", "CREATE", 100, "{\"groupId\":\"100\",\"groupName\":\"Admins\"}"),
            change(7, "USERGROUP", "CREATE", 101, "{\"groupId\":\"101\",\"groupName\":\"Devs\"}")));
    }

    /** Enqueue the bootstrap page and run one cycle. */
    private void bootstrap() throws SQLException {
        enqueueBootstrap();
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
    }

    private static String change(long id, String entityType, String action, long entityId,
                                 String sccal) {
        return TestSupport.changeEntry(id, entityType, action, entityId, sccal, "{}");
    }

    private static String entry(long seq, long lastChangeId) {
        return TestSupport.cursorEntry(seq, CUST, TEN, lastChangeId);
    }

    private long count(String table) throws SQLException {
        return TestSupport.count(st, table);
    }

    private String workspaceName(long workspaceId) throws SQLException {
        return TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.workspace WHERE workspace_id = " + workspaceId);
    }
}
