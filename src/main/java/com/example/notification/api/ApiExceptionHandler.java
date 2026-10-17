package com.example.notification.api;

import com.example.notification.ratelimit.RateLimitExceededException;
import com.example.notification.service.IdempotencyConflictException;
import com.example.notification.service.NotificationNotFoundException;
import com.example.notification.web.ApiAuthenticationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.TypeMismatchException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Turns every error into an RFC 9457 problem response ({@code application/problem+json})
 * with a {@code detail} a client can act on: which field or parameter, what value was
 * sent, and what is allowed instead.
 *
 * <p>Spring MVC's own exceptions (bad parameters, unreadable bodies, validation, 404/405/
 * 415) come through {@link ResponseEntityExceptionHandler}; the overrides below only
 * sharpen their detail. Field-level failures also carry an {@code errors} list of
 * {@code {field, message}}.
 *
 * <p>Nothing internal leaks: details are built from the request, never from an exception
 * message, and an unexpected exception becomes a generic 500 carrying an {@code errorId}
 * that matches the ERROR line logged with its stack trace.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    /** Long enough to recognise the value a client sent, short enough not to echo a payload back. */
    private static final int MAX_ECHOED_VALUE = 100;

    @ExceptionHandler(NotificationNotFoundException.class)
    public ProblemDetail handleNotFound(NotificationNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(ApiAuthenticationException.class)
    public ProblemDetail handleUnauthenticated(ApiAuthenticationException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNAUTHORIZED, e.getMessage());
    }

    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<ProblemDetail> handleRateLimited(RateLimitExceededException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.TOO_MANY_REQUESTS,
                "Rate limit of %d requests per minute exceeded. Retry after %d second(s)."
                        .formatted(e.getLimitPerMinute(), e.getRetryAfterSeconds()));
        problem.setProperty("retryAfterSeconds", e.getRetryAfterSeconds());
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(e.getRetryAfterSeconds()))
                .body(problem);
    }

    @ExceptionHandler(InvalidRequestException.class)
    public ProblemDetail handleInvalidRequest(InvalidRequestException e) {
        return badRequest(List.of(new FieldProblem(e.getField(), e.getMessage())));
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ProblemDetail handleIdempotencyConflict(IdempotencyConflictException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    }

    /**
     * Anything not handled above is a bug or an outage, not the client's fault. The
     * client gets a generic 500 and an id; the log gets the stack trace under that id.
     */
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception e, HttpServletRequest request) {
        String errorId = UUID.randomUUID().toString();
        log.error("Unhandled error {} on {} {}", errorId, request.getMethod(), request.getRequestURI(), e);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred. Quote errorId " + errorId + " when reporting it.");
        problem.setProperty("errorId", errorId);
        return problem;
    }

    // ---- sharper detail for Spring MVC's own exceptions ----

    /** {@code ?status=FAILED}, {@code ?limit=abc}, a non-UUID path id. */
    @Override
    protected ResponseEntity<Object> handleTypeMismatch(TypeMismatchException ex, HttpHeaders headers,
                                                        HttpStatusCode status, WebRequest request) {
        String name = ex instanceof MethodArgumentTypeMismatchException m ? m.getName() : ex.getPropertyName();
        ProblemDetail problem = badRequest(List.of(new FieldProblem(name,
                invalidValue(ex.getValue(), ex.getRequiredType()))));
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    /** {@code @Valid} body failures: every field that failed, not just the first. */
    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
                                                                  HttpHeaders headers, HttpStatusCode status,
                                                                  WebRequest request) {
        List<FieldProblem> problems = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> new FieldProblem(error.getField(), error.getDefaultMessage()))
                .sorted(Comparator.comparing(FieldProblem::field).thenComparing(FieldProblem::message))
                .toList();
        return handleExceptionInternal(ex, badRequest(problems), headers, status, request);
    }

    /** Malformed JSON, a wrong type, or an unknown enum value in the body. */
    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
                                                                  HttpHeaders headers, HttpStatusCode status,
                                                                  WebRequest request) {
        ProblemDetail problem;
        Throwable cause = ex.getCause();
        if (cause instanceof MismatchedInputException mismatch && !mismatch.getPath().isEmpty()) {
            Object value = mismatch instanceof InvalidFormatException format ? format.getValue() : null;
            problem = badRequest(List.of(new FieldProblem(path(mismatch), invalidValue(value, mismatch.getTargetType()))));
        } else if (cause instanceof JsonProcessingException json && json.getLocation() != null) {
            problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                    "Malformed JSON request body (line %d, column %d)."
                            .formatted(json.getLocation().getLineNr(), json.getLocation().getColumnNr()));
        } else {
            problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                    "Request body is missing or is not valid JSON.");
        }
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    /** Unknown path. Spring's own wording ("No static resource ...") reads like a file server. */
    @Override
    protected ResponseEntity<Object> handleNoResourceFoundException(NoResourceFoundException ex,
                                                                    HttpHeaders headers, HttpStatusCode status,
                                                                    WebRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND,
                "No endpoint %s /%s.".formatted(ex.getHttpMethod(), ex.getResourcePath()));
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    // ---- helpers ----

    /** One failing field or parameter, as reported in a problem's {@code errors} list. */
    record FieldProblem(String field, String message) {
    }

    private static ProblemDetail badRequest(List<FieldProblem> problems) {
        String summary = problems.stream()
                .map(p -> "'" + p.field() + "' " + p.message())
                .collect(Collectors.joining("; "));
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "Invalid request: " + summary + ".");
        problem.setProperty("errors", problems.stream()
                .map(p -> Map.of("field", p.field(), "message", p.message()))
                .toList());
        return problem;
    }

    /** "has invalid value 'FAILED'; expected one of PENDING, SENDING, SENT, DEAD". */
    private static String invalidValue(Object value, Class<?> expectedType) {
        String sent = value == null ? "has an invalid value" : "has invalid value '" + truncate(value) + "'";
        return sent + "; expected " + describe(expectedType);
    }

    private static String describe(Class<?> type) {
        if (type == null) {
            return "a different value";
        }
        if (type.isEnum()) {
            return "one of " + Arrays.stream(type.getEnumConstants())
                    .map(constant -> ((Enum<?>) constant).name())
                    .collect(Collectors.joining(", "));
        }
        if (type == UUID.class) {
            return "a UUID";
        }
        if (type == int.class || type == Integer.class || type == long.class || type == Long.class) {
            return "an integer";
        }
        if (type == String.class) {
            return "a string";
        }
        return "a " + type.getSimpleName();
    }

    private static String path(JsonMappingException e) {
        return e.getPath().stream()
                .map(ref -> ref.getFieldName() != null ? ref.getFieldName() : "[" + ref.getIndex() + "]")
                .collect(Collectors.joining(".")).replace(".[", "[");
    }

    private static String truncate(Object value) {
        String text = String.valueOf(value);
        return text.length() <= MAX_ECHOED_VALUE ? text : text.substring(0, MAX_ECHOED_VALUE) + "…";
    }
}
