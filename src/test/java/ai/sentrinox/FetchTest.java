package ai.sentrinox;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Mockito tests for {@link SccalReferenceSync#fetchAll}: request shape, the
 * one-request-per-type fan-out and HTTP error handling — all without a server.
 */
class FetchTest {

    private static HttpResponse<String> response(int status, String body) {
        return TestSupport.response(status, body, URI.create("http://stub/x"));
    }

    private static HttpClient clientReturning(HttpResponse<String> resp) {
        return TestSupport.httpStub(req -> resp);
    }

    @Test
    void issuesOneRequestPerTypeWithCustomerAndTenantParams() {
        HttpClient http = clientReturning(response(200, "{\"1\":{}}"));

        Map<String, String> bodies = SccalReferenceSync.fetchAll(
            http, "http://sccal:8080", List.of("user", "workspace"), 111L, 222L);

        assertEquals(2, bodies.size());
        assertTrue(bodies.containsKey("user"));
        assertTrue(bodies.containsKey("workspace"));

        ArgumentCaptor<HttpRequest> reqs = ArgumentCaptor.forClass(HttpRequest.class);
        verify(http, times(2)).sendAsync(reqs.capture(), any(HttpResponse.BodyHandler.class));
        List<String> urls = reqs.getAllValues().stream().map(r -> r.uri().toString()).toList();
        assertTrue(urls.stream().anyMatch(u ->
            u.equals("http://sccal:8080/internal/sccal/api/v1/user/list?customerId=111&tenantId=222")), urls.toString());
        assertTrue(urls.stream().anyMatch(u -> u.contains("/workspace/list?customerId=111&tenantId=222")), urls.toString());
    }

    @Test
    void blankBodyBecomesEmptyJsonObject() {
        HttpClient http = clientReturning(response(200, ""));

        Map<String, String> bodies = SccalReferenceSync.fetchAll(
            http, "http://sccal:8080", List.of("user"), 1L, 2L);

        assertEquals("{}", bodies.get("user"));
    }

    @Test
    void nonOkStatusFailsLoudly() {
        HttpClient http = clientReturning(response(404, "nope"));

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            SccalReferenceSync.fetchAll(http, "http://sccal:8080", List.of("user"), 1L, 2L));
        assertTrue(ex.getMessage().contains("HTTP 404"), ex.getMessage());
    }
}
