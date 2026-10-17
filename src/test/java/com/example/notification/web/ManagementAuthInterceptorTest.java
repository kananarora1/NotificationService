package com.example.notification.web;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;

class ManagementAuthInterceptorTest {

    @Test
    void rejectsARequestWithoutTheOperatorTokenWhenEnabled() {
        MockHttpServletResponse response = new MockHttpServletResponse();

        boolean allowed = interceptor(true).preHandle(new MockHttpServletRequest(), response, new Object());

        assertThat(allowed).isFalse();
        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void acceptsOnlyTheConfiguredOperatorTokenWhenEnabled() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(ManagementAuthInterceptor.MANAGEMENT_TOKEN_HEADER, "operator-secret");

        assertThat(interceptor(true).preHandle(request, new MockHttpServletResponse(), new Object())).isTrue();
    }

    @Test
    void leavesLocalDevelopmentUnrestrictedWhenDisabled() {
        assertThat(interceptor(false).preHandle(new MockHttpServletRequest(), new MockHttpServletResponse(), new Object()))
                .isTrue();
    }

    private static ManagementAuthInterceptor interceptor(boolean enabled) {
        return new ManagementAuthInterceptor(new ManagementSecurityProperties(enabled, "operator-secret"));
    }
}
