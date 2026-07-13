package ai.sentrinox;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Shared HTTP plumbing for the SCCAL JSON endpoints: client construction, the
 * request shape (timeouts, Accept header), the join/unwrap of async sends, and
 * the 200-with-coalesced-body contract the capture path relies on.
 */
final class SccalHttp {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    /** Client with the shared connect timeout; built once per process and reused. */
    static HttpClient newClient() {
        return HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
    }

    /** GET request for a JSON endpoint, with the shared timeout and Accept header. */
    private static HttpRequest jsonGet(String url) {
        return HttpRequest.newBuilder(URI.create(url))
            .timeout(REQUEST_TIMEOUT)
            .header("Accept", "application/json")
            .GET()
            .build();
    }

    /** Blocking GET of {@code url}; transport failures surface as RuntimeException. */
    static HttpResponse<String> get(HttpClient http, String url) {
        try {
            return http.send(jsonGet(url), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new RuntimeException("GET " + url + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("GET " + url + " interrupted", e);
        }
    }

    /** Fail loudly unless HTTP 200; returns the coalesced body. */
    static String requireOk(HttpResponse<String> resp) {
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("GET " + resp.uri() + " -> HTTP " + resp.statusCode());
        }
        return coalesceBody(resp.body());
    }

    /**
     * Blank/absent 200 bodies coalesce to {@code "{}"} — the contract the
     * empty-stage delete guard relies on; both capture paths must use this.
     */
    private static String coalesceBody(String body) {
        return (body == null || body.isBlank()) ? "{}" : body;
    }

    private SccalHttp() {
    }
}
