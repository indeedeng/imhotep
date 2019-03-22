package com.indeed.imhotep.matcher;

import com.google.common.base.Preconditions;
import com.indeed.flamdex.api.StringTermIterator;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Consumer;

public class ExactStringTermMatcher implements StringTermMatcher {
    private final String patternString;
    private final byte[] patternBytes;

    ExactStringTermMatcher(final String pattern) {
        Preconditions.checkArgument(!pattern.isEmpty());
        this.patternString = pattern;
        this.patternBytes = patternString.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean matches(final String term) {
        return patternString.equals(term);
    }

    @Override
    public boolean matches(final byte[] termBytes, final int termBytesLength) {
        if (termBytesLength != patternBytes.length) {
            return false;
        }
        return Arrays.equals(patternBytes, termBytes);
    }

    @Override
    public void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch) {
        termIterator.reset(patternString);
        if (termIterator.next()) {
            if (matches(termIterator.termStringBytes(), termIterator.termStringLength())) {
                onMatch.accept(termIterator);
            }
        }
    }
}
