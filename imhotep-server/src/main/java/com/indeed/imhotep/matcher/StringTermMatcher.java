package com.indeed.imhotep.matcher;

import com.google.common.base.Charsets;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.imhotep.automaton.Automaton;

import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class StringTermMatcher {
    private static final String ANY_STRING_PATTERN = "\\.\\*";
    private static final String CAPTURED_NON_SPECIAL_NONEMPTY_STRING = "([^|&?*+{}~\\[\\].#@\"()<>\\\\^$]+)";
    private static final String ALL_MATCH = ".*";
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
        if (ALL_MATCH.equals(regex)) {
            return new AllMatchStringTermMatcher();
        }

        if (StringMatcherUtil.isWellFormedString(Charsets.UTF_8, regex)) {
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
        }
        return new AutomatonStringTermMatcher(regex);
    }

}
