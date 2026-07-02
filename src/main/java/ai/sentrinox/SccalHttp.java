package ai.sentrinox;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Shared HTTP plumbing for the SCCAL JSON endpoints: client construction, the
 * request shape (timeouts, Accept header), the join/unwrap of async sends, and
 * the 200-with-coalesced-body contract both capture paths rely on. The one
 * home for these — neither sync class talks to {@link HttpClient} directly
 * beyond issuing sends.
 */
final class SccalHttp {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    /** Client with the shared connect timeout; built once per process and reused. */
    static HttpClient newClient() {
        return HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
    }

    /** GET request for a JSON endpoint, with the shared timeout and Accept header. */
    static HttpRequest jsonGet(String url) {
        return HttpRequest.newBuilder(URI.create(url))
            .timeout(REQUEST_TIMEOUT)
            .header("Accept", "application/json")
            .GET()
            .build();
    }

    /** Blocking GET of {@code url}; transport failures surface as RuntimeException. */
    static HttpResponse<String> get(HttpClient http, String url) {
        return join(http.sendAsync(jsonGet(url), HttpResponse.BodyHandlers.ofString()),
            "GET " + url + " failed");
    }

    /** Join an async send, unwrapping the CompletionException into {@code message}. */
    static HttpResponse<String> join(CompletableFuture<HttpResponse<String>> inflight,
                                     String message) {
        try {
            return inflight.join();
        } catch (CompletionException ce) {
            throw new RuntimeException(message, ce.getCause() != null ? ce.getCause() : ce);
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
    static String coalesceBody(String body) {
        return (body == null || body.isBlank()) ? "{}" : body;
    }

    private SccalHttp() {
    }
}
