package com.indeed.imhotep.local;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.imhotep.automaton.Automaton;
import com.indeed.imhotep.automaton.RegExp;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class StringTermMatcher {
    private static final String ANY_STRING_PATTERN = "\\.\\*";
    private static final String CAPTURED_NON_SPECIAL_NONEMPTY_STRING = "([-_,a-zA-Z0-9]+)";
    private static final Pattern PREFIX_MATCH = Pattern.compile(CAPTURED_NON_SPECIAL_NONEMPTY_STRING + ANY_STRING_PATTERN);
    private static final Pattern SUFFIX_MATCH = Pattern.compile(ANY_STRING_PATTERN + CAPTURED_NON_SPECIAL_NONEMPTY_STRING);
    private static final Pattern INCLUDE_MATCH = Pattern.compile(ANY_STRING_PATTERN + CAPTURED_NON_SPECIAL_NONEMPTY_STRING + ANY_STRING_PATTERN);

    /**
     * Returns true iff the given term matches to the pattern
     */
    public abstract boolean matches(final String term);

    /**
     * For every term matches to the pattern, calls {@code onMatch#accept} with the given {@code termIterator}
     * pointing the corresponding term position.
     */
    public abstract void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch);

    /**
     * Returns an implementation of {@link StringTermMatcher} for the given regular expression,
     * possibly that of optimized version compared to the fallback implementation using {@link Automaton}.
     */
    public static StringTermMatcher forRegex(final String regex) {
        Matcher matcher;

        matcher = PREFIX_MATCH.matcher(regex);
        if (matcher.matches()) {
            return new PrefixStringTermMatcher(matcher.group(1));
        }
        matcher = SUFFIX_MATCH.matcher(regex);
        if (matcher.matches()) {
            return new SuffixStringTermMatcher(matcher.group(1));
        }
        matcher = INCLUDE_MATCH.matcher(regex);
        if (matcher.matches()) {
            return new IncludeStringTermMatcher(matcher.group(1));
        }
        return new AutomatonStringTermMatcher(regex);
    }

    /**
     * Build KMP table (strict border) for the given pattern
     */
    @VisibleForTesting
    static int[] buildKMPTable(final byte[] pattern) {
        Preconditions.checkArgument(pattern.length > 0);
        final int[] table = new int[pattern.length + 1];
        table[0] = -1;
        int failureLink = 0;
        for (int i = 1; i < pattern.length; ++i) {
            if (pattern[i] == pattern[failureLink]) {
                table[i] = table[failureLink];
            } else {
                table[i] = failureLink;
                while ((failureLink >= 0) && (pattern[i] != pattern[failureLink])) {
                    failureLink = table[failureLink];
                }
            }
            ++failureLink;
        }
        table[pattern.length] = failureLink;
        return table;
    }

    @VisibleForTesting
    static class PrefixStringTermMatcher extends StringTermMatcher {
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

                matchLength = Math.min(matchLength, termIterator.commonPrefixLengthWithPrevious());
                final byte[] term = termIterator.termStringBytes();
                // In case termIterator.commonPrefixLengthWithPrevious returned a number smaller than the actual common prefix length.
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

    @VisibleForTesting
    static class SuffixStringTermMatcher extends StringTermMatcher {
        private final String patternString;
        private final byte[] pattern;
        private final int[] kmpTable;

        SuffixStringTermMatcher(final String pattern) {
            Preconditions.checkArgument(!pattern.isEmpty());
            this.patternString = pattern;
            this.pattern = pattern.getBytes(Charsets.UTF_8);
            this.kmpTable = StringTermMatcher.buildKMPTable(this.pattern);
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
                int pos = termIterator.commonPrefixLengthWithPrevious();

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

    @VisibleForTesting
    static class IncludeStringTermMatcher extends StringTermMatcher {
        private final String patternString;
        private final byte[] pattern;
        private final int[] kmpTable;

        IncludeStringTermMatcher(final String pattern) {
            Preconditions.checkArgument(!pattern.isEmpty());
            this.patternString = pattern;
            this.pattern = pattern.getBytes(Charsets.UTF_8);
            this.kmpTable = StringTermMatcher.buildKMPTable(this.pattern);
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

    @VisibleForTesting
    static class AutomatonStringTermMatcher extends StringTermMatcher {
        private final Automaton automaton;

        AutomatonStringTermMatcher(final String regex) {
            this.automaton = new RegExp(regex).toAutomaton();
        }

        @Override
        public boolean matches(final String term) {
            return automaton.run(term);
        }

        @Override
        public void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch) {
            // TODO: Automaton#getCommonPrefix and reset termIterator.
            // TODO: Do something similar to stateStack.
            // TODO: Determine sink state and eagerly stop evaluating automaton.
            while (termIterator.next()) {
                final String term = termIterator.term();
                if (automaton.run(term)) {
                    onMatch.accept(termIterator);
                }
            }
        }
    }
}
