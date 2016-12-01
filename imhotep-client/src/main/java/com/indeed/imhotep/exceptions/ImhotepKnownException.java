package com.indeed.imhotep.exceptions;

/**
 * @author vladimir
 */

public abstract class ImhotepKnownException extends RuntimeException {
    public ImhotepKnownException() {
        super();
    }

    public ImhotepKnownException(String message) {
        super(message);
    }

    public ImhotepKnownException(String message, Throwable cause) {
        super(message, cause);
    }

    public ImhotepKnownException(Throwable cause) {
        super(cause);
    }

    protected ImhotepKnownException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
