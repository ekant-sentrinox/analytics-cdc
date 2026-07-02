package ai.sentrinox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end capture test against a real in-memory DuckDB, with the SCCAL API
 * stubbed by Mockito. Exercises the three CDC operations plus idempotency,
 * using the project's actual V1/V6/V7 schema SQL.
 *
 * <p>Snapshot-diff semantics are asserted on {@code workspace} (still
 * snapshot-fed); {@code user} is stream-fed and only reaches the snapshot path
 * on first-run bootstrap — its steady-state behaviour is covered by
 * {@link ChangeStreamCaptureTest}. The stub answers {@code /cursors} and
 * {@code /changes} with an empty body, so the change stream bootstraps on the
 * first run and is idle after that.
 */
class SccalReferenceSyncCaptureTest {

    private Connection conn;
    private Statement st;
    /** objectType -> canned JSON the stub returns; tests mutate this between runs. */
    private final Map<String, String> responses = new HashMap<>();
    /** HTTP status the stub returns; tests flip this to simulate a failing cycle. */
    private int stubStatus = 200;
    private HttpClient http;

    @BeforeEach
    void setUp() throws Exception {
        // Stand the catalog up as a plain in-memory db (no DuckLake/MinIO needed).
        conn = TestSupport.openLake();
        st = conn.createStatement();
        TestSupport.runSqlFile(st, "ollylake/init/V6__reference_tables.sql");
        TestSupport.runSqlFile(st, "ollylake/init/V1__create_customer_tenant_ids_table.sql");
        TestSupport.runSqlFile(st, "ollylake/init/V7__sync_cursor_state.sql");

        responses.put("user", "{\"10\":{\"userId\":\"10\",\"userName\":\"a@x.com\"},"
            + "\"20\":{\"userId\":\"20\",\"userName\":\"b@x.com\"},"
            + "\"30\":{\"userId\":\"30\",\"userName\":\"c@x.com\"}}");
        responses.put("usergroup", "{\"100\":{\"userGroupId\":\"100\",\"name\":\"Admins\"}}");
        responses.put("usergroupmembership", "{\"10:100\":{\"userId\":\"10\",\"userGroupId\":\"100\"}}");
        responses.put("workspace", "{\"1\":{\"workspaceId\":\"1\",\"name\":\"ws-1\"},"
            + "\"2\":{\"workspaceId\":\"2\",\"name\":\"ws-2\"}}");
        responses.put("providercatalogue", "{\"500\":{\"providerCatalogueId\":\"500\",\"name\":\"Anthropic\",\"type\":1},"
            + "\"501\":{\"providerCatalogueId\":\"501\",\"name\":\"OpenAI\",\"type\":2}}");
        // One rule per ruleType: 0 = TenantProvider, 1 = WorkspaceProvider
        // (both feed llm_access_rule), 2 = TenantMcp, 3 = WorkspaceMcp
        // (both feed mcp_access_rule).
        responses.put("rule", "{\"a\":{\"ruleId\":\"900\",\"name\":\"Allow\",\"ruleType\":0},"
            + "\"b\":{\"ruleId\":\"901\",\"name\":\"Block\",\"ruleType\":1},"
            + "\"c\":{\"ruleId\":\"902\",\"name\":\"MCP Allow\",\"ruleType\":2},"
            + "\"d\":{\"ruleId\":\"903\",\"name\":\"MCP WS Allow\",\"ruleType\":3}}");
        http = stubClient();
    }

    @AfterEach
    void tearDown() throws SQLException {
        st.close();
        conn.close();
    }

    @Test
    void fullSnapshotInsertsEveryTable() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count("ollylake.main.workspace"));
        assertEquals(3, count("ollylake.main.\"user\""));
        assertEquals(1, count("ollylake.main.\"group\""));
        assertEquals(1, count("ollylake.main.user_group_mapping"));
        assertEquals(2, count("ollylake.main.provider"));
        assertEquals(2, count("ollylake.main.llm_access_rule"));
        assertEquals(2, count("ollylake.main.mcp_access_rule"));
    }

    @Test
    void reRunWithUnchangedDataIsIdempotent() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(3, count("ollylake.main.\"user\""));
        assertEquals(2, count("ollylake.main.workspace"));
    }

    @Test
    void capturesAttributeUpdate() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals("ws-2", workspaceName(2));

        responses.put("workspace", "{\"1\":{\"workspaceId\":\"1\",\"name\":\"ws-1\"},"
            + "\"2\":{\"workspaceId\":\"2\",\"name\":\"ws-renamed\"}}");   // upstream rename
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals("ws-renamed", workspaceName(2));
        assertEquals(2, count("ollylake.main.workspace"));         // no spurious insert
    }

    @Test
    void capturesDeleteWhenRowDropsFromSnapshot() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals(2, count("ollylake.main.workspace"));

        // Workspace 2 no longer present in the snapshot.
        responses.put("workspace", "{\"1\":{\"workspaceId\":\"1\",\"name\":\"ws-1\"}}");
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1, count("ollylake.main.workspace"));
    }

    @Test
    void capturesDeleteFromTombstone() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        responses.put("workspace", "{\"1\":{\"workspaceId\":\"1\",\"name\":\"ws-1\"},"
            + "\"2\":{\"workspaceId\":\"2\",\"name\":\"ws-2\",\"isDeleted\":true}}");
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1, count("ollylake.main.workspace"));
    }

    @Test
    void emptySnapshotDoesNotDeleteExistingRows() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals(2, count("ollylake.main.workspace"));

        // A degenerate-but-successful response: an empty snapshot for workspaces.
        // Without the empty-stage guard this would delete both rows.
        responses.put("workspace", "{}");
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count("ollylake.main.workspace"));
    }

    @Test
    void blankBodySnapshotDoesNotDeleteExistingRows() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals(2, count("ollylake.main.workspace"));

        // Blank body is coalesced to "{}" by fetchAll, then guarded like any empty stage.
        responses.put("workspace", "");
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count("ollylake.main.workspace"));
    }

    @Test
    void pollCycleSurvivesFailureAndRecoversNextCycle() throws SQLException {
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

    private HttpClient stubClient() {
        return TestSupport.httpStub(req -> TestSupport.response(stubStatus,
            responses.getOrDefault(TestSupport.objectTypeOf(req.uri()), "{}"), req.uri()));
    }

    private long count(String table) throws SQLException {
        return TestSupport.count(st, table);
    }

    private String workspaceName(long workspaceId) throws SQLException {
        return TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.workspace WHERE workspace_id = " + workspaceId);
    }
}
