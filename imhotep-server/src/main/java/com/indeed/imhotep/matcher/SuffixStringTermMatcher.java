package com.indeed.imhotep.matcher;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.indeed.flamdex.api.StringTermIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.function.Consumer;

class SuffixStringTermMatcher implements StringTermMatcher {
    private final String patternString;
    private final byte[] pattern;
    private final int[] kmpTable;

    SuffixStringTermMatcher(final String pattern) {
        Preconditions.checkArgument(!pattern.isEmpty());
        this.patternString = pattern;
        this.pattern = pattern.getBytes(Charsets.UTF_8);
        this.kmpTable = StringMatcherUtil.buildKMPTable(this.pattern);
    }

    @Override
    public boolean matches(final String term) {
        return term.endsWith(patternString);
    }

    @Override
    public void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch) {
        // The state for term[pos] will be stored in stateStack[pos - 1].
        final IntArrayList stateStack = new IntArrayList();
        stateStack.push(0);

        while (termIterator.next()) {
            // Invariant: stateStack.size() == previousTermLength + 1.

            final int termLength = termIterator.termStringLength();
            final byte[] term = termIterator.termStringBytes();
            int pos = termIterator.commonPrefixLengthWithPreviousLowerBound();

            int state = stateStack.getInt(pos);

            // stateStack[0 <= i <= pos] is also valid for this term.
            stateStack.ensureCapacity(termLength + 1);
            stateStack.size(pos + 1);

            while (pos < termLength) {
                while ((state >= 0) && (pattern[state] != term[pos])) {
                    state = kmpTable[state];
                }
                ++state;
                ++pos;
                if (state == pattern.length) {
                    // term[0 <= i < pos] <= term < (any terms we'll see in future).
                    // Thus, we don't need to keep track of match info.
                    state = kmpTable[state];
                    if (pos == termLength) {
                        onMatch.accept(termIterator);
                    }
                }
                stateStack.push(state);
            }
        }
    }
}
