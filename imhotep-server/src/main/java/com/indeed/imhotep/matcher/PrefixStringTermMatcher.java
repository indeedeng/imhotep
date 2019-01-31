package com.indeed.imhotep.matcher;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.indeed.flamdex.api.StringTermIterator;

import java.util.function.Consumer;

class PrefixStringTermMatcher implements StringTermMatcher {
    private final String patternString;
    private final byte[] pattern;

    PrefixStringTermMatcher(final String pattern) {
        Preconditions.checkArgument(!pattern.isEmpty());
        this.patternString = pattern;
        this.pattern = pattern.getBytes(Charsets.UTF_8);
    }

    @Override
    public boolean matches(final String term) {
        return term.startsWith(patternString);
    }

    @Override
    public void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch) {
        termIterator.reset(patternString);
        int matchLength = 0;
        while (termIterator.next()) {
            final int termLen = termIterator.termStringLength();
            if (termLen < pattern.length) {
                return;
            }

            matchLength = Math.min(matchLength, termIterator.commonPrefixLengthWithPreviousLowerBound());
            final byte[] term = termIterator.termStringBytes();
            // In case termIterator.commonPrefixLengthWithPreviousLowerBound returned a number smaller than the actual common prefix length.
            while (matchLength < pattern.length) {
                if (term[matchLength] == pattern[matchLength]) {
                    matchLength++;
                } else {
                    return;
                }
            }
            onMatch.accept(termIterator);
        }
    }
}