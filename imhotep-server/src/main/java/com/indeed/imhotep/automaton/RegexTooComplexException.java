package com.indeed.imhotep.automaton;

public class RegexTooComplexException extends RuntimeException {
    public RegexTooComplexException() {
        super();
    }

    public RegexTooComplexException(final String s) {
        super(s);
    }

    public RegexTooComplexException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
