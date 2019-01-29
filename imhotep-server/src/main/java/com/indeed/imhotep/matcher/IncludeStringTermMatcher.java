package com.indeed.imhotep.matcher;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.indeed.flamdex.api.StringTermIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.function.Consumer;

class IncludeStringTermMatcher extends StringTermMatcher {
    private final String patternString;
    private final byte[] pattern;
    private final int[] kmpTable;

    IncludeStringTermMatcher(final String pattern) {
        Preconditions.checkArgument(!pattern.isEmpty());
        this.patternString = pattern;
        this.pattern = pattern.getBytes(Charsets.UTF_8);
        this.kmpTable = StringMatcherUtil.buildKMPTable(this.pattern);
    }

    @Override
    public boolean matches(final String term) {
        // Current usage is for
        return term.contains(patternString);
    }

    @Override
    public void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch) {
        // The state for term[pos] will be stored in stateStack[pos - 1] (if the stack has that element).
        // The last element could be pattern.length. Other states are non-match.
        final IntArrayList stateStack = new IntArrayList();
        stateStack.push(0);

        while (termIterator.next()) {
            final int termLength = termIterator.termStringLength();
            final byte[] term = termIterator.termStringBytes();
            int pos = termIterator.commonPrefixLengthWithPrevious();
            pos = Math.min(pos, stateStack.size() - 1);

            // stateStack[0 <= i <= pos] is also valid for this term.
            stateStack.ensureCapacity(termLength + 1);
            stateStack.size(pos + 1);
            int state = stateStack.getInt(pos);

            if (state == pattern.length) {
                // It's already matched
                onMatch.accept(termIterator);
                continue;
            }

            while (pos < termLength) {
                while ((state >= 0) && (pattern[state] != term[pos])) {
                    state = kmpTable[state];
                }
                ++state;
                ++pos;
                stateStack.push(state);
                if (state == pattern.length) {
                    onMatch.accept(termIterator);
                    break;
                }
            }
        }
    }
}
