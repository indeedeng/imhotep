package com.indeed.imhotep.matcher;

import com.google.common.base.Charsets;
import com.indeed.imhotep.automaton.Automaton;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringTermMatchers {
    private static final String ANY_STRING_PATTERN = Pattern.quote(".*");
    private static final String CAPTURED_NON_SPECIAL_NONEMPTY_STRING = "([^|&?*+{}~\\[\\].#@\"()<>\\\\^$]+)";
    private static final String ALL_MATCH = ".*";
    private static final Pattern PREFIX_MATCH = Pattern.compile(CAPTURED_NON_SPECIAL_NONEMPTY_STRING + ANY_STRING_PATTERN);
    private static final Pattern SUFFIX_MATCH = Pattern.compile(ANY_STRING_PATTERN + CAPTURED_NON_SPECIAL_NONEMPTY_STRING);
    private static final Pattern INCLUDE_MATCH = Pattern.compile(ANY_STRING_PATTERN + CAPTURED_NON_SPECIAL_NONEMPTY_STRING + ANY_STRING_PATTERN);

    private StringTermMatchers() {
    }

    /**
     * Returns an implementation of {@link StringTermMatcher} for the given regular expression,
     * possibly that of optimized version compared to the fallback implementation using {@link Automaton}.
     */
    public static StringTermMatcher forRegex(final String regex) {
        if (ALL_MATCH.equals(regex)) {
            return new AllMatchStringTermMatcher();
        }

        if (StringMatcherUtil.isWellFormedString(Charsets.UTF_8, regex)) {
            final Matcher prefixMatcher = PREFIX_MATCH.matcher(regex);
            if (prefixMatcher.matches()) {
                return new PrefixStringTermMatcher(prefixMatcher.group(1));
            }
            final Matcher suffixMatcher = SUFFIX_MATCH.matcher(regex);
            if (suffixMatcher.matches()) {
                return new SuffixStringTermMatcher(suffixMatcher.group(1));
            }
            final Matcher includeMatcher = INCLUDE_MATCH.matcher(regex);
            if (includeMatcher.matches()) {
                return new IncludeStringTermMatcher(includeMatcher.group(1));
            }
        }
        return new AutomatonStringTermMatcher(regex);
    }
}
