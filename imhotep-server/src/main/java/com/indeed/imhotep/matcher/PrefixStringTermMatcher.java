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
        // NOTE:
        // StringTermIterator iterates over UTF-8 byte arrays,
        // but where they're sorted as UTF-16 code unit array (i.e. String#compareTo).
        // Though, the binary relation "isPrefixOf" won't change by UTF-8 / UTF-16 / UTF-32 (=UnicodeScalarSequence),
        // i.e. if there are (valid) Unicode Scalar sequences S and T,
        // isPrefixOf(S, T) = isPrefixOf(S.encode(UTF-8), T.encode(UTF-8)) = isPrefixOf(S.encode(UTF-16), T.encode(UTF-16)).
        //
        // This means that the the string terms those have `pattern` as a prefix are in a contiguous range of
        // the term iterator.

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
