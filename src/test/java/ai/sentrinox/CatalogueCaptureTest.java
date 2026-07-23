package ai.sentrinox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for the catalogue {@code /list} capture path
 * ({@link CatalogueListSync}) against a real in-memory DuckDB, with the
 * SCCAL endpoints stubbed. Covers the bootstrap snapshot, the gated
 * {@code updatedSince} incremental with tombstones, the cursor-advance rules,
 * the per-pair tenant snapshot and poison-entry isolation.
 */
class CatalogueCaptureTest {

    private static final Instant NOW = Instant.parse("2026-07-22T12:00:00Z");
    /** What a cursor advanced at {@link #NOW} must read: NOW minus the 60s lap. */
    private static final String NOW_CURSOR = "2026-07-22T11:59:00Z";

    private static final String PROVIDER_TABLE = "ollylake.main.provider";
    private static final String MODEL_TABLE = "ollylake.main.model";
    private static final String TENANT_TABLE = "ollylake.main.tenant";

    private Connection conn;
    private Statement st;
    private HttpClient http;
    private final TestSupport.SccalStub stub = new TestSupport.SccalStub();

    @BeforeEach
    void setUp() throws Exception {
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

    // ---- bootstrap -------------------------------------------------------------

    @Test
    void bootstrapSnapshotSeedsTablesCursorsAndRevision() throws SQLException {
        stub.addListResponse("providercatalogue",
            "{\"901\":" + provider(901, "OpenAI", "LLM", false) + "}");
        stub.addListResponse("modelcatalogue",
            "{\"1001\":" + model(1001, "gpt-5", 901, false) + "}");
        stub.addListResponse("mcpservercatalogue",
            "{\"2001\":{\"mcpServerCatalogueId\":\"2001\",\"name\":\"Filesystem\","
                + "\"serverUrl\":\"https://mcp.acme/fs\",\"category\":\"files\","
                + "\"risk\":\"LOW\",\"defaultAccessMode\":\"READ\",\"isDeleted\":false}}");
        stub.addListResponse("mcptoolcatalogue",
            "{\"3001\":{\"mcpToolCatalogueId\":\"3001\",\"name\":\"read_file\","
                + "\"description\":\"Reads a file\",\"mcpServerCatalogueId\":\"2001\","
                + "\"isDeleted\":false}}");

        CatalogueListSync.run(conn, st, http, "http://stub", 3L, NOW);

        assertEquals("OpenAI", TestSupport.queryString(st,
            "SELECT name FROM " + PROVIDER_TABLE + " WHERE provider_id = 901"));
        assertEquals(901, TestSupport.queryLong(st,
            "SELECT provider_id FROM " + MODEL_TABLE + " WHERE model_id = 1001"));
        assertEquals("Filesystem", TestSupport.queryString(st,
            "SELECT name FROM ollylake.main.mcp_server WHERE mcp_server_id = 2001"));
        assertEquals(2001, TestSupport.queryLong(st,
            "SELECT mcp_server_id FROM ollylake.main.mcp_tool WHERE mcp_tool_id = 3001"));

        // Bootstrap pulls are full snapshots: no updatedSince on any request.
        assertTrue(listRequests().stream().noneMatch(u -> u.getQuery().contains("updatedSince")));
        // Cursor advanced to NOW - lap for every global type; gate revision stored.
        assertEquals(NOW_CURSOR, cursor("providercatalogue"));
        assertEquals(NOW_CURSOR, cursor("modelcatalogue"));
        assertEquals(NOW_CURSOR, cursor("mcpservercatalogue"));
        assertEquals(NOW_CURSOR, cursor("mcptoolcatalogue"));
        assertEquals(3, revision());
    }

    @Test
    void emptyBootstrapStoresTheRevisionButNoCursors() throws SQLException {
        // Gate known but every catalogue empty: no rows → no cursors written
        // (the next real pull must still be a full snapshot), but the gate
        // revision IS marked seen — an idle empty system must not re-hit
        // /list every cycle...
        CatalogueListSync.run(conn, st, http, "http://stub", 3L, NOW);

        assertEquals(0, TestSupport.count(st, CatalogueListSync.CURSOR_TABLE));
        assertEquals(3, revision());

        // ...so the next cycle at the same revision skips the endpoints.
        stub.requests.clear();
        CatalogueListSync.run(conn, st, http, "http://stub", 3L, NOW);
        assertEquals(0, listRequests().size());
    }

    // ---- incremental ------------------------------------------------------------

    @Test
    void gateBumpPullsDeltaWithCursorAndAppliesTombstone() throws SQLException {
        bootstrap();

        // A global migration landed (revision 3 → 4): the delta carries an
        // update, an insert and a soft-delete tombstone.
        stub.requests.clear();
        stub.addListResponse("modelcatalogue",
            "{\"1001\":" + model(1001, "gpt-5-turbo", 901, false)
                + ",\"1002\":" + model(1002, "claude", 902, false)
                + ",\"1003\":" + model(1003, "retired-model", 901, true) + "}");
        st.execute("INSERT INTO " + MODEL_TABLE + " VALUES (1003, 'retired-model', 901, false)");

        Instant later = NOW.plusSeconds(600);
        CatalogueListSync.run(conn, st, http, "http://stub", 4L, later);

        assertEquals("gpt-5-turbo", TestSupport.queryString(st,
            "SELECT name FROM " + MODEL_TABLE + " WHERE model_id = 1001"));
        assertTrue(TestSupport.exists(st, "SELECT 1 FROM " + MODEL_TABLE
            + " WHERE model_id = 1002 AND NOT is_deleted"));
        // Tombstone: the row is kept and flagged, not removed.
        assertTrue(TestSupport.exists(st, "SELECT 1 FROM " + MODEL_TABLE
            + " WHERE model_id = 1003 AND is_deleted"));

        // The delta request carried the bootstrap cursor as updatedSince.
        assertTrue(listRequests().stream().anyMatch(u ->
            u.getPath().endsWith("/modelcatalogue/list")
                && u.getQuery().contains("updatedSince=" + NOW_CURSOR)));
        // Non-empty pull advanced the model cursor; an empty one (providers)
        // left its cursor untouched.
        assertEquals("2026-07-22T12:09:00Z", cursor("modelcatalogue"));
        assertEquals(NOW_CURSOR, cursor("providercatalogue"));
        assertEquals(4, revision());
    }

    @Test
    void unchangedGateSkipsGlobalPulls() throws SQLException {
        bootstrap();

        stub.requests.clear();
        CatalogueListSync.run(conn, st, http, "http://stub", 3L, NOW.plusSeconds(30));

        assertTrue(listRequests().isEmpty(), "gate unchanged — no /list request expected");
        assertEquals(NOW_CURSOR, cursor("providercatalogue"));
        assertEquals(3, revision());
    }

    @Test
    void nullGatePullsEveryCycleAndStoresNoRevision() throws SQLException {
        // A server predating the gate: pull every cycle (correct, unoptimized).
        CatalogueListSync.run(conn, st, http, "http://stub", null, NOW);
        assertEquals(4, listRequests().size());
        assertEquals(0, TestSupport.count(st, CatalogueListSync.REVISION_TABLE));

        stub.requests.clear();
        stub.addListResponse("providercatalogue",
            "{\"901\":" + provider(901, "OpenAI", "LLM", false) + "}");
        CatalogueListSync.run(conn, st, http, "http://stub", null, NOW.plusSeconds(120));

        assertEquals(1, TestSupport.count(st, PROVIDER_TABLE));
        assertEquals("2026-07-22T12:01:00Z", cursor("providercatalogue"));
        assertEquals(0, TestSupport.count(st, CatalogueListSync.REVISION_TABLE));
    }

    @Test
    void replayedSnapshotIsIdempotentAndNeverMovesCursorBackwards() throws SQLException {
        bootstrap();

        // The same provider row replays, and the wall clock has regressed
        // (skew correction): candidate = NOW - 10s - lap < the saved cursor.
        // The merge must converge and the cursor must hold, not move back.
        stub.addListResponse("providercatalogue",
            "{\"901\":" + provider(901, "OpenAI", "LLM", false) + "}");
        CatalogueListSync.run(conn, st, http, "http://stub", 4L, NOW.minusSeconds(10));

        assertEquals(1, TestSupport.count(st, PROVIDER_TABLE));
        assertEquals("OpenAI", TestSupport.queryString(st,
            "SELECT name FROM " + PROVIDER_TABLE + " WHERE provider_id = 901"));
        assertEquals(NOW_CURSOR, cursor("providercatalogue"));    // max(saved, candidate)
    }

    @Test
    void keylessPoisonEntryIsSkippedNotWedged() throws SQLException {
        // One entry lacks its key (and one has a non-numeric key): both are
        // skipped; the good row applies and the cycle commits normally.
        stub.addListResponse("providercatalogue",
            "{\"900\":{\"name\":\"no-id\",\"type\":\"LLM\",\"isDeleted\":false},"
                + "\"nan\":{\"providerCatalogueId\":\"not-a-number\",\"name\":\"bad\"},"
                + "\"901\":" + provider(901, "OpenAI", "LLM", false) + "}");

        CatalogueListSync.run(conn, st, http, "http://stub", 3L, NOW);

        assertEquals(1, TestSupport.count(st, PROVIDER_TABLE));
        assertTrue(TestSupport.exists(st,
            "SELECT 1 FROM " + PROVIDER_TABLE + " WHERE provider_id = 901"));
    }

    // ---- tenant (per-pair snapshot) ----------------------------------------------

    @Test
    void tenantPullsPerKnownPairAndStampsCustomerId() throws SQLException {
        st.execute("INSERT INTO ollylake.main.sccal_change_cursor VALUES (1, 2, 5)");
        st.execute("INSERT INTO ollylake.main.sccal_change_cursor VALUES (3, 4, 9)");
        stub.addListResponse("tenant", "{\"2\":" + tenant(2, "acme") + "}");
        stub.addListResponse("tenant", "{\"4\":" + tenant(4, "globex") + "}");

        CatalogueListSync.run(conn, st, http, "http://stub", 3L, NOW);

        assertEquals(2, TestSupport.count(st, TENANT_TABLE));
        assertEquals("acme", TestSupport.queryString(st, "SELECT name FROM " + TENANT_TABLE
            + " WHERE tenant_id = 2 AND customer_id = 1"));
        assertEquals("globex", TestSupport.queryString(st, "SELECT name FROM " + TENANT_TABLE
            + " WHERE tenant_id = 4 AND customer_id = 3"));
        // One tenant/list call per pair, each with the pair's own context.
        List<URI> tenantCalls = listRequests().stream()
            .filter(u -> u.getPath().endsWith("/tenant/list")).toList();
        assertEquals(2, tenantCalls.size());
        assertTrue(tenantCalls.stream().anyMatch(u ->
            u.getQuery().contains("customerId=1") && u.getQuery().contains("tenantId=2")));
        // Snapshot-only endpoint: never sent an updatedSince, and no cursor row.
        assertTrue(tenantCalls.stream().noneMatch(u -> u.getQuery().contains("updatedSince")));
        assertFalse(TestSupport.exists(st, "SELECT 1 FROM " + CatalogueListSync.CURSOR_TABLE
            + " WHERE object_type = 'tenant'"));
    }

    @Test
    void tenantRenameIsCapturedEvenWhenGateIsUnchanged() throws SQLException {
        st.execute("INSERT INTO ollylake.main.sccal_change_cursor VALUES (1, 2, 5)");
        stub.addListResponse("tenant", "{\"2\":" + tenant(2, "acme") + "}");
        CatalogueListSync.run(conn, st, http, "http://stub", 3L, NOW);
        assertEquals("acme", tenantName(2));

        // Tenant data is per-customer, not global: the catalog gate must not
        // gate it. Same revision, renamed tenant → captured.
        stub.requests.clear();
        stub.addListResponse("tenant", "{\"2\":" + tenant(2, "acme-renamed") + "}");
        CatalogueListSync.run(conn, st, http, "http://stub", 3L, NOW.plusSeconds(30));

        assertEquals("acme-renamed", tenantName(2));
        assertEquals(1, listRequests().size());     // tenant only — globals skipped
        assertTrue(listRequests().get(0).getPath().endsWith("/tenant/list"));
    }

    // ---- end to end through runOnce ------------------------------------------------

    @Test
    void runOnceDrivesCataloguePullFromTheCursorsPageGate() throws SQLException {
        // One cycle: the /cursors page advances a tenant AND carries the gate;
        // the stream applies the user, the catalogue pass pulls the provider,
        // and the tenant dimension row lands for the discovered pair.
        stub.cursorsJson = TestSupport.cursorsPage(TestSupport.cursorEntry(5, 1, 2, 42), 6, 3);
        stub.changesResponses.add(TestSupport.changesPage(43, false,
            TestSupport.changeEntry(42, "USER", "CREATE", 10,
                "{\"userId\":\"10\",\"userName\":\"a@x.com\"}", "{}")));
        stub.addListResponse("providercatalogue",
            "{\"901\":" + provider(901, "OpenAI", "LLM", false) + "}");
        stub.addListResponse("tenant", "{\"2\":" + tenant(2, "acme") + "}");

        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1, TestSupport.count(st, "ollylake.main.\"user\""));
        assertEquals(1, TestSupport.count(st, PROVIDER_TABLE));
        assertEquals("acme", tenantName(2));
        assertEquals(3, revision());
        // The captured tenant dimension resolves tenant_name in v_ai_audit
        // (the view serves the stream's audit rows for the same pair).
        assertEquals("acme", TestSupport.queryString(st,
            "SELECT tenant_name FROM ollylake.main.v_ai_audit"
                + " WHERE customer_id = 1 AND tenant_id = 2"));

        // Second cycle, nothing new anywhere: gate unchanged → no global /list
        // calls; only the per-pair tenant snapshot re-runs (and no-ops).
        stub.cursorsJson = TestSupport.cursorsPage("", 6, 3);
        stub.requests.clear();
        stub.addListResponse("tenant", "{\"2\":" + tenant(2, "acme") + "}");
        SccalReferenceSync.runOnce(conn, st, http, "http://stub");

        assertEquals(1, listRequests().size());
        assertTrue(listRequests().get(0).getPath().endsWith("/tenant/list"));
        assertEquals(1, TestSupport.count(st, TENANT_TABLE));
    }

    // ---- helpers -------------------------------------------------------------

    /** Seed provider 901 and model 1001 at gate revision 3, cursors at NOW - lap. */
    private void bootstrap() throws SQLException {
        stub.addListResponse("providercatalogue",
            "{\"901\":" + provider(901, "OpenAI", "LLM", false) + "}");
        stub.addListResponse("modelcatalogue",
            "{\"1001\":" + model(1001, "gpt-5", 901, false) + "}");
        CatalogueListSync.run(conn, st, http, "http://stub", 3L, NOW);
        assertEquals(1, TestSupport.count(st, PROVIDER_TABLE));
        assertEquals(3, revision());
    }

    private static String provider(long id, String name, String type, boolean deleted) {
        return "{\"providerCatalogueId\":\"" + id + "\",\"name\":\"" + name
            + "\",\"type\":\"" + type + "\",\"isDeleted\":" + deleted + "}";
    }

    private static String model(long id, String name, long providerId, boolean deleted) {
        return "{\"modelCatalogueId\":\"" + id + "\",\"name\":\"" + name
            + "\",\"providerCatalogueId\":\"" + providerId + "\",\"isDeleted\":" + deleted + "}";
    }

    private static String tenant(long id, String name) {
        return "{\"tenantId\":\"" + id + "\",\"name\":\"" + name + "\",\"isDeleted\":false}";
    }

    /** Every recorded /list request. */
    private List<URI> listRequests() {
        return stub.requests.stream().filter(u -> u.getPath().endsWith("/list")).toList();
    }

    private String cursor(String objectType) throws SQLException {
        return TestSupport.queryString(st, "SELECT next_updated_since FROM "
            + CatalogueListSync.CURSOR_TABLE + " WHERE object_type = '" + objectType + "'");
    }

    private long revision() throws SQLException {
        return TestSupport.queryLong(st,
            "SELECT schema_revision FROM " + CatalogueListSync.REVISION_TABLE);
    }

    private String tenantName(long tenantId) throws SQLException {
        return TestSupport.queryString(st,
            "SELECT name FROM " + TENANT_TABLE + " WHERE tenant_id = " + tenantId);
    }
}
