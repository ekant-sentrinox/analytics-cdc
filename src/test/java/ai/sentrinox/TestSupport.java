package ai.sentrinox;

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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

/**
 * Shared scaffolding for the capture tests: the in-memory stand-in for the
 * ollylake catalog, migration-file execution, single-value query helpers and
 * the Mockito HTTP stubs. Routing logic stays in each test — only the
 * boilerplate lives here.
 */
final class TestSupport {

    /** In-memory DuckDB with an attached {@code ':memory:'} catalog named ollylake. */
    static Connection openLake() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("ATTACH ':memory:' AS ollylake");
            st.execute("USE ollylake");
        }
        return conn;
    }

    /**
     * Run every ollylake init migration in version order — the same
     * ordering the production initializer uses, so a new migration is picked
     * up by every capture test without editing each fixture.
     *
     * <p>The init SQL lives in the analytics-schema repo (the CDC migrations
     * merged into its {@code ollylake/init} chain), vendored here as the
     * {@code schema/} submodule. {@code OLLYLAKE_INIT_DIR} overrides the
     * directory (e.g. to test against an external checkout).
     */
    static void runInitMigrations(Statement st) throws Exception {
        String initDir = System.getenv()
            .getOrDefault("OLLYLAKE_INIT_DIR", "schema/ollylake/init");
        for (java.nio.file.Path file : OllylakeSchemaInitializer.sqlFiles(Paths.get(initDir))) {
            runSqlFile(st, file.toString());
        }
    }

    /**
     * Run one project SQL file (e.g. an {@code ollylake/init} migration),
     * statement by statement.
     *
     * <p>DuckLake physical-layout syntax ({@code ALTER TABLE ... SET
     * PARTITIONED BY (...)} / {@code SET SORTED BY (...)}) is dropped: the
     * plain in-memory catalog {@link #openLake()} stands up cannot parse or
     * execute it. Layout is a concern of the production DuckLake catalog and
     * does not affect what these correctness tests exercise.
     */
    static void runSqlFile(Statement st, String path) throws Exception {
        for (String stmt : SqlScripts.splitStatements(Files.readString(Paths.get(path)))) {
            String upper = stmt.toUpperCase(java.util.Locale.ROOT);
            if (upper.contains("SET PARTITIONED BY") || upper.contains("SET SORTED BY")) {
                continue;
            }
            st.execute(stmt);
        }
    }

    static long count(Statement st, String table) throws SQLException {
        return queryLong(st, "SELECT count(*) FROM " + table);
    }

    /** First column of the query's first row as a long; the row must exist. */
    static long queryLong(Statement st, String sql) throws SQLException {
        return SqlScripts.queryLong(st, sql);
    }

    /** First column of the query's first row as a string; the row must exist. */
    static String queryString(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    /** True when the query returns at least one row. */
    static boolean exists(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            return rs.next();
        }
    }

    /** Mockito HttpClient whose {@code send} answers each request via {@code router}. */
    @SuppressWarnings("unchecked")
    static HttpClient httpStub(Function<HttpRequest, HttpResponse<String>> router) {
        HttpClient client = mock(HttpClient.class);
        try {
            lenient().when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenAnswer(inv -> router.apply(inv.getArgument(0)));
        } catch (Exception impossible) {
            // send() declares checked exceptions; stubbing never throws them.
            throw new AssertionError(impossible);
        }
        return client;
    }

    // ---- SCCAL endpoint stub + JSON page builders (shared by the capture tests) ----

    /**
     * Stub of the SCCAL endpoints: {@code /cursors} answers from
     * {@link #cursorsJson} (any startOffset); {@code /changes} answers from a
     * FIFO of responses — a String is an HTTP 200 body, an Integer a bare
     * status; {@code /{objectType}/list} answers from a per-objectType FIFO
     * ({@link #addListResponse}), defaulting to the empty map {@code "{}"}.
     * Every request URI is recorded in {@link #requests} so tests can assert
     * which endpoints were hit and with which query params. A non-200
     * {@link #status} fails every endpoint (for failed-cycle tests).
     */
    static final class SccalStub {
        String cursorsJson = "{}";
        final Deque<Object> changesResponses = new ArrayDeque<>();
        final Map<String, Deque<String>> listResponses = new HashMap<>();
        final List<URI> requests = new ArrayList<>();
        int status = 200;

        /** Queue one /list body for the given objectType. */
        void addListResponse(String objectType, String body) {
            listResponses.computeIfAbsent(objectType, k -> new ArrayDeque<>()).add(body);
        }

        HttpClient client() {
            return httpStub(req -> {
                URI uri = req.uri();
                requests.add(uri);
                if (status != 200) {
                    return response(status, "", uri);
                }
                String path = uri.getPath();
                if (path.endsWith("/cursors")) {
                    return response(200, cursorsJson, uri);
                }
                if (path.endsWith("/changes")) {
                    Object next = changesResponses.isEmpty() ? "{}" : changesResponses.poll();
                    return next instanceof Integer failure
                        ? response(failure, "", uri)
                        : response(200, (String) next, uri);
                }
                if (path.endsWith("/list")) {
                    String[] segments = path.split("/");
                    Deque<String> queue = listResponses.get(segments[segments.length - 2]);
                    String body = (queue == null || queue.isEmpty()) ? "{}" : queue.poll();
                    return response(200, body, uri);
                }
                throw new AssertionError(
                    "unexpected endpoint (only /cursors, /changes and /list exist): " + path);
            });
        }
    }

    /** JSON for one /cursors registry entry. */
    static String cursorEntry(long seq, long customerId, long tenantId, long lastChangeId) {
        return "{\"seq\":\"" + seq + "\",\"customerId\":\"" + customerId + "\",\"tenantId\":\""
            + tenantId + "\",\"lastChangeId\":\"" + lastChangeId + "\",\"lastActivationId\":\"9\","
            + "\"updatedAt\":\"2026-07-01T00:00:00Z\"}";
    }

    /** One /cursors page (hasMore false) around the given entries CSV. */
    static String cursorsPage(String entriesCsv, long nextOffset) {
        return "{\"entries\":[" + entriesCsv + "],\"startOffset\":1,\"limit\":1000,\"nextOffset\":"
            + nextOffset + ",\"hasMore\":false}";
    }

    /** One /cursors page carrying the embedded global-catalog gate revision. */
    static String cursorsPage(String entriesCsv, long nextOffset, long schemaRevision) {
        return "{\"entries\":[" + entriesCsv + "],\"startOffset\":1,\"limit\":1000,\"nextOffset\":"
            + nextOffset + ",\"hasMore\":false,\"globalCatalog\":{\"schemaRevision\":"
            + schemaRevision + ",\"appliedAt\":\"2026-07-20T00:00:00Z\"}}";
    }

    /** One /changes page around the given entry JSONs. */
    static String changesPage(long nextOffset, boolean hasMore, String... entries) {
        return "{\"entries\":[" + String.join(",", entries) + "],\"startOffset\":1,\"limit\":1000,"
            + "\"nextOffset\":" + nextOffset + ",\"hasMore\":" + hasMore + "}";
    }

    /** JSON for one /changes entry with the given sccal and human payloads. */
    static String changeEntry(long id, String entityType, String action, long entityId,
                              String sccalJson, String humanJson) {
        return "{\"id\":\"" + id + "\",\"entityType\":\"" + entityType + "\",\"action\":\""
            + action + "\",\"entityId\":\"" + entityId + "\",\"activationId\":\"77\","
            + "\"txnId\":\"88\",\"changedAt\":\"2026-07-01T00:00:00Z\",\"changedBy\":\"99\","
            + "\"sccal\":" + sccalJson + ",\"human\":" + humanJson + "}";
    }

    /** Canned HttpResponse with the given status, body and request URI. */
    @SuppressWarnings("unchecked")
    static HttpResponse<String> response(int status, String body, URI uri) {
        HttpResponse<String> resp = mock(HttpResponse.class);
        lenient().when(resp.statusCode()).thenReturn(status);
        lenient().when(resp.body()).thenReturn(body);
        lenient().when(resp.uri()).thenReturn(uri);
        return resp;
    }

    private TestSupport() {
    }
}
