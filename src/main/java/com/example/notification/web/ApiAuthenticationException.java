package com.example.notification.web;

/**
 * The request has no usable API key. Thrown by {@link ApiKeyAuthInterceptor} and turned
 * into a 401 problem response by {@code ApiExceptionHandler}.
 */
public class ApiAuthenticationException extends RuntimeException {

    public ApiAuthenticationException(String detail) {
        super(detail);
    }
}
