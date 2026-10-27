package com.example.notification.web;

import com.example.notification.provider.BrevoProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;

class BrevoWebhookAuthFilterTest {

    @Test
    void acceptsOnlyTheConfiguredBasicCredentialForWebhookPath() throws Exception {
        BrevoWebhookAuthFilter filter = filter("relaynest", "webhook-secret");
        MockHttpServletRequest request = request("/webhooks/brevo", "Basic cmVsYXluZXN0OndlYmhvb2stc2VjcmV0");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(chain.getRequest()).isSameAs(request);
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void rejectsMissingOrWrongCredentialsWithoutInvokingController() throws Exception {
        BrevoWebhookAuthFilter filter = filter("relaynest", "webhook-secret");
        MockHttpServletResponse missingResponse = new MockHttpServletResponse();
        MockFilterChain missingChain = new MockFilterChain();
        filter.doFilter(request("/webhooks/brevo", null), missingResponse, missingChain);

        MockHttpServletResponse wrongResponse = new MockHttpServletResponse();
        MockFilterChain wrongChain = new MockFilterChain();
        filter.doFilter(request("/webhooks/brevo", "Basic cmVsYXluZXN0Ondyb25n"), wrongResponse, wrongChain);

        assertThat(missingResponse.getStatus()).isEqualTo(401);
        assertThat(missingChain.getRequest()).isNull();
        assertThat(wrongResponse.getStatus()).isEqualTo(401);
        assertThat(wrongChain.getRequest()).isNull();
    }

    @Test
    void doesNotRequireWebhookCredentialsForOtherApplicationEndpoints() throws Exception {
        BrevoWebhookAuthFilter filter = filter("relaynest", "webhook-secret");
        MockHttpServletRequest request = request("/actuator/health", null);
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(chain.getRequest()).isSameAs(request);
    }

    private static BrevoWebhookAuthFilter filter(String username, String password) {
        return new BrevoWebhookAuthFilter(new BrevoProperties(
                "https://api.brevo.com/v3/smtp/email", "api-key", "notifications@relaynest.tech",
                "RelayNest", "", "RelayNest notification", username, password));
    }

    private static MockHttpServletRequest request(String path, String authorization) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", path);
        request.setServletPath(path);
        if (authorization != null) {
            request.addHeader("Authorization", authorization);
        }
        return request;
    }
}
