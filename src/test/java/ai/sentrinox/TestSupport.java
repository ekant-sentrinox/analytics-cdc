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
import java.util.concurrent.CompletableFuture;
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

    /** Run one project SQL file (e.g. an {@code ollylake/init} migration), statement by statement. */
    static void runSqlFile(Statement st, String path) throws Exception {
        for (String stmt : SqlScripts.splitStatements(Files.readString(Paths.get(path)))) {
            st.execute(stmt);
        }
    }

    static long count(Statement st, String table) throws SQLException {
        return queryLong(st, "SELECT count(*) FROM " + table);
    }

    /** First column of the query's first row as a long; the row must exist. */
    static long queryLong(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
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

    /** Mockito HttpClient whose {@code sendAsync} answers each request via {@code router}. */
    @SuppressWarnings("unchecked")
    static HttpClient httpStub(Function<HttpRequest, HttpResponse<String>> router) {
        HttpClient client = mock(HttpClient.class);
        lenient().when(client.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenAnswer(inv -> CompletableFuture.completedFuture(router.apply(inv.getArgument(0))));
        return client;
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

    /** {@code .../api/v1/<objectType>/list} → {@code <objectType>}. */
    static String objectTypeOf(URI uri) {
        String[] parts = uri.getPath().split("/");
        return parts[parts.length - 2];
    }

    private TestSupport() {
    }
}
