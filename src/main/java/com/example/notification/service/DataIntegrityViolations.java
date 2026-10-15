package com.example.notification.service;

import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;

/** Tells apart the integrity violations that mean something specific to the caller. */
public final class DataIntegrityViolations {

    private DataIntegrityViolations() {
    }

    /**
     * Whether {@code e} was caused by the named constraint. A broader check — any
     * {@link DataIntegrityViolationException} — would also match a value too long for its
     * column, a NOT NULL or a CHECK failure, and misread each as whatever the caller
     * expected the constraint to mean.
     */
    public static boolean isViolationOf(DataIntegrityViolationException e, String constraintName) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof ConstraintViolationException violation) {
                return constraintName.equalsIgnoreCase(violation.getConstraintName());
            }
        }
        return false;
    }
}
