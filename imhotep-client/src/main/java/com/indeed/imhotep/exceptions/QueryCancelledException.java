package com.indeed.imhotep.exceptions;

/**
 * @author vladimir
 */

public class QueryCancelledException extends ImhotepKnownException {
    public QueryCancelledException() {
    super();
    }

    public QueryCancelledException(String s) {
        super(s);
    }

    public QueryCancelledException(String message, Throwable cause) {
            super(message, cause);
        }
}
