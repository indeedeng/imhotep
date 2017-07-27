package com.indeed.imhotep.exceptions;

/**
 * @author vladimir
 */

public abstract class ImhotepKnownException extends RuntimeException {
    public ImhotepKnownException() {
        super();
    }

    public ImhotepKnownException(final String message) {
        super(message);
    }

    public ImhotepKnownException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ImhotepKnownException(final Throwable cause) {
        super(cause);
    }

    protected ImhotepKnownException(
            final String message,
            final Throwable cause,
            final boolean enableSuppression,
            final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
