package ai.sentrinox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end capture test against a real in-memory DuckDB, with the SCCAL API
 * stubbed by Mockito. Exercises the three CDC operations plus idempotency,
 * using the project's actual V1/V6 schema SQL.
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
        conn = DriverManager.getConnection("jdbc:duckdb:");
        st = conn.createStatement();
        // Stand the catalog up as a plain in-memory db (no DuckLake/MinIO needed).
        st.execute("ATTACH ':memory:' AS ollylake");
        st.execute("USE ollylake");
        runSqlFile("ollylake/init/V6__reference_tables.sql");
        runSqlFile("ollylake/init/V1__create_customer_tenant_ids_table.sql");

        responses.put("user", userJson("b@x.com", false));
        responses.put("usergroup", "{\"100\":{\"userGroupId\":\"100\",\"name\":\"Admins\"}}");
        responses.put("usergroupmembership", "{\"10:100\":{\"userId\":\"10\",\"userGroupId\":\"100\"}}");
        responses.put("workspace", "{\"1\":{\"workspaceId\":\"1\",\"name\":\"ws-1\"},"
            + "\"2\":{\"workspaceId\":\"2\",\"name\":\"ws-2\"}}");
        responses.put("providercatalogue", "{\"500\":{\"providerCatalogueId\":\"500\",\"name\":\"Anthropic\",\"type\":1},"
            + "\"501\":{\"providerCatalogueId\":\"501\",\"name\":\"OpenAI\",\"type\":2}}");
        responses.put("rule", "{\"a\":{\"ruleId\":\"900\",\"name\":\"Allow\",\"ruleType\":1},"
            + "\"b\":{\"ruleId\":\"901\",\"name\":\"Block\",\"ruleType\":1},"
            + "\"c\":{\"ruleId\":\"902\",\"name\":\"MCP Allow\",\"ruleType\":3}}");

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
        assertEquals(1, count("ollylake.main.mcp_access_rule"));
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
        assertEquals("b@x.com", userName(20));

        responses.put("user", userJson("renamed@x.com", false));   // upstream rename
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals("renamed@x.com", userName(20));
        assertEquals(3, count("ollylake.main.\"user\""));          // no spurious insert
    }

    @Test
    void capturesDeleteWhenRowDropsFromSnapshot() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals(3, count("ollylake.main.\"user\""));

        // User 30 no longer present in the snapshot.
        responses.put("user", "{\"10\":{\"userId\":\"10\",\"userName\":\"a@x.com\"},"
            + "\"20\":{\"userId\":\"20\",\"userName\":\"b@x.com\"}}");
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count("ollylake.main.\"user\""));
    }

    @Test
    void capturesDeleteFromTombstone() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        responses.put("user", userJson("b@x.com", true));          // user 30 isDeleted:true
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(2, count("ollylake.main.\"user\""));
    }

    @Test
    void emptySnapshotDoesNotDeleteExistingRows() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals(3, count("ollylake.main.\"user\""));

        // A degenerate-but-successful response: an empty snapshot for users.
        // Without the empty-stage guard this would delete all three rows.
        responses.put("user", "{}");
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(3, count("ollylake.main.\"user\""));
    }

    @Test
    void blankBodySnapshotDoesNotDeleteExistingRows() throws SQLException {
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");
        assertEquals(3, count("ollylake.main.\"user\""));

        // Blank body is coalesced to "{}" by fetchAll, then guarded like any empty stage.
        responses.put("user", "");
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(3, count("ollylake.main.\"user\""));
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

    /** Three users; user 30's isDeleted flag and user 20's name are parameterised. */
    private static String userJson(String name20, boolean user30Deleted) {
        String u30 = user30Deleted
            ? "\"30\":{\"userId\":\"30\",\"userName\":\"c@x.com\",\"isDeleted\":true}"
            : "\"30\":{\"userId\":\"30\",\"userName\":\"c@x.com\"}";
        return "{\"10\":{\"userId\":\"10\",\"userName\":\"a@x.com\"},"
            + "\"20\":{\"userId\":\"20\",\"userName\":\"" + name20 + "\"},"
            + u30 + "}";
    }

    @SuppressWarnings("unchecked")
    private HttpClient stubClient() {
        HttpClient client = mock(HttpClient.class);
        lenient().when(client.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenAnswer(inv -> {
                HttpRequest req = inv.getArgument(0);
                String objectType = objectTypeOf(req.uri());
                HttpResponse<String> resp = mock(HttpResponse.class);
                lenient().when(resp.statusCode()).thenReturn(stubStatus);
                lenient().when(resp.body()).thenReturn(responses.getOrDefault(objectType, "{}"));
                lenient().when(resp.uri()).thenReturn(req.uri());
                return CompletableFuture.completedFuture(resp);
            });
        return client;
    }

    /** .../api/v1/<objectType>/list -> <objectType>. */
    private static String objectTypeOf(URI uri) {
        String[] parts = uri.getPath().split("/");
        return parts[parts.length - 2];
    }

    private void runSqlFile(String path) throws Exception {
        String sql = Files.readString(Paths.get(path));
        for (String stmt : SccalReferenceSync.splitStatements(sql)) {
            st.execute(stmt);
        }
    }

    private long count(String table) throws SQLException {
        try (ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private String userName(long userId) throws SQLException {
        try (ResultSet rs = st.executeQuery(
            "SELECT name FROM ollylake.main.\"user\" WHERE user_id = " + userId)) {
            rs.next();
            return rs.getString(1);
        }
    }
}
