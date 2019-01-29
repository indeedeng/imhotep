package com.indeed.imhotep.matcher;

import com.google.common.base.Preconditions;
import com.indeed.flamdex.api.StringTermIterator;

import java.util.function.Consumer;

public class ExactStringTermMatcher implements StringTermMatcher {
    private final String patternString;

    ExactStringTermMatcher(final String pattern) {
        Preconditions.checkArgument(!pattern.isEmpty());
        this.patternString = pattern;
    }

    @Override
    public boolean matches(final String term) {
        return patternString.equals(term);
    }

    @Override
    public void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch) {
        termIterator.reset(patternString);
        if (termIterator.next()) {
            if (patternString.equals(termIterator.term())) {
                onMatch.accept(termIterator);
            }
        }
    }
}
