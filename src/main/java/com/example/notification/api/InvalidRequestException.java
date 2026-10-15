package com.example.notification.api;

/**
 * A request input that bean validation does not cover (a header, say) is invalid. Turned
 * into a 400 problem response naming the field by {@link ApiExceptionHandler}.
 */
public class InvalidRequestException extends RuntimeException {

    private final String field;

    public InvalidRequestException(String field, String message) {
        super(message);
        this.field = field;
    }

    public String getField() {
        return field;
    }
}
