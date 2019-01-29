package com.indeed.imhotep.matcher;

import com.indeed.flamdex.api.StringTermIterator;

import java.util.function.Consumer;

public interface StringTermMatcher {

    /**
     * Returns true iff the given term matches to the pattern
     */
    boolean matches(final String term);

    /**
     * For every term matches to the pattern, calls {@code onMatch#accept} with the given {@code termIterator}
     * pointing the corresponding term position.
     */
    void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch);
}
