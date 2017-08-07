package com.indeed.imhotep.exceptions;

/**
 * @author vladimir
 */

public class QueryCancelledException extends ImhotepKnownException {
    public QueryCancelledException() {
    super();
    }

    public QueryCancelledException(final String s) {
        super(s);
    }

    public QueryCancelledException(final String message, final Throwable cause) {
            super(message, cause);
        }
}
