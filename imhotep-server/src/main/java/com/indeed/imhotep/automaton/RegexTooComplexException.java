package com.indeed.imhotep.automaton;

public class RegexTooComplexException extends RuntimeException {
    public RegexTooComplexException() {
        super();
    }

    public RegexTooComplexException(String s) {
        super(s);
    }

    public RegexTooComplexException(String message, Throwable cause) {
        super(message, cause);
    }
}
