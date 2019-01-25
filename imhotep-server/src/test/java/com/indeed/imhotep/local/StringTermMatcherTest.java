package com.indeed.imhotep.local;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.api.StringTermIterator;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StringTermMatcherTest {
    private static class MockStringTermIterator implements StringTermIterator {
        private final boolean returnZeroInsteadOfLCP;
        private final List<String> terms;
        private int currentPos = 0;

        private MockStringTermIterator(final boolean returnZeroInsteadOfLCP, final Collection<String> terms) {
            this.returnZeroInsteadOfLCP = returnZeroInsteadOfLCP;
            this.terms = terms.stream().sorted().collect(Collectors.toList());
        }

        @Override
        public void reset(final String term) {
            currentPos = Collections.binarySearch(terms, term);
            if (currentPos < 0) {
                currentPos = (~currentPos) - 1;
            } else {
                --currentPos;
            }
        }

        @Override
        public String term() {
            return terms.get(currentPos);
        }

        @Override
        public byte[] termStringBytes() {
            return term().getBytes(Charsets.UTF_8);
        }

        @Override
        public int termStringLength() {
            return termStringBytes().length;
        }

        @Override
        public int commonPrefixLengthWithPrevious() {
            if ((currentPos == 0) || returnZeroInsteadOfLCP) {
                return 0;
            } else {
                final byte[] previous = terms.get(currentPos - 1).getBytes(Charsets.UTF_8);
                final byte[] current = termStringBytes();
                for (int i = 0; (i < previous.length) && (i < current.length); ++i) {
                    if (current[i] != previous[i]) {
                        return i;
                    }
                }
                return previous.length;
            }
        }

        @Override
        public boolean next() {
            ++currentPos;
            return currentPos < terms.size();
        }

        @Override
        public int docFreq() {
            return 0;
        }

        @Override
        public void close() {
        }
    }

    private static Set<String> getAllMatch(final StringTermMatcher stringTermMatcher, final Set<String> terms) {
        final Set<String> runEachIndividually = terms.stream().filter(stringTermMatcher::matches).collect(Collectors.toSet());
        try (
                final MockStringTermIterator iterator = new MockStringTermIterator(false, terms);
                final MockStringTermIterator iteratorWithZeroLCP = new MockStringTermIterator(true, terms);
        ) {
            final Set<String> actualForIterator = new HashSet<>();
            stringTermMatcher.run(iterator, it -> actualForIterator.add(it.term()));
            final Set<String> actualForIteratorWithZeroLCP = new HashSet<>();
            stringTermMatcher.run(iteratorWithZeroLCP, it -> actualForIteratorWithZeroLCP.add(it.term()));

            assertEquals(runEachIndividually, actualForIterator);
            assertEquals(runEachIndividually, actualForIteratorWithZeroLCP);
            return actualForIterator;
        }
    }

    @Test
    public void testAlterImplementation() {
        assertThat(
                StringTermMatcher.forRegex("foo.*"),
                instanceOf(StringTermMatcher.PrefixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*foo"),
                instanceOf(StringTermMatcher.SuffixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*foo.*"),
                instanceOf(StringTermMatcher.IncludeStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*foo|bar.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("[f]oobar.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("f.o.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("fo+.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("fo{1,2}.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("foo?.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("foo&bar.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("foo~a.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("[.].*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("<1-3>.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
    }

    @Test
    public void testKMPTable() {
        // Examples from https://en.wikipedia.org/wiki/Knuth–Morris–Pratt_algorithm
        assertArrayEquals(
                new int[]{-1, 0, 0, 0, -1, 0, 2, 0},
                StringTermMatcher.buildKMPTable("ABCDABD".getBytes(Charsets.UTF_8))
        );
        assertArrayEquals(
                new int[]{-1, 0, -1, 1, -1, 0, -1, 3, 2, 0},
                StringTermMatcher.buildKMPTable("ABACABABC".getBytes(Charsets.UTF_8))
        );
        assertArrayEquals(
                new int[]{-1, 0, -1, 1, -1, 0, -1, 3, -1, 3},
                StringTermMatcher.buildKMPTable("ABACABABA".getBytes(Charsets.UTF_8))
        );
        assertArrayEquals(
                new int[]{-1, 0, 0, 0, 0, 0, 0, -1, 0, 2, 0, 0, 0, 0, 0, -1, 0, 0, 3, 0, 0, 0, 0, 0, 0},
                StringTermMatcher.buildKMPTable("PARTICIPATE IN PARACHUTE".getBytes(Charsets.UTF_8))
        );
    }

    @Test
    public void testPrefixMatch() {
        assertEquals(
                ImmutableSet.of(
                        "prefix", "prefixed"
                ),
                getAllMatch(
                        new StringTermMatcher.PrefixStringTermMatcher("prefix"),
                        ImmutableSet.of(
                                "", "p", "prefix", "prefixed", "prepared", "query", "this is a prefix"
                        )
                )
        );
        assertEquals(
                ImmutableSet.of(
                        "prefixed", "prefixed by prefix"
                ),
                getAllMatch(
                        new StringTermMatcher.PrefixStringTermMatcher("prefix"),
                        ImmutableSet.of(
                                "", "p", "prefixed", "prefixed by prefix", "prepared", "query", "this is a prefix"
                        )
                )
        );
    }

    @Test
    public void testSuffixMatch() {
        assertEquals(
                ImmutableSet.of(
                        "suffix", "suffix of this string is suffix"
                ),
                getAllMatch(
                        new StringTermMatcher.SuffixStringTermMatcher("suffix"),
                        ImmutableSet.of(
                                "", "prefix", "s", "suffix", "suffix of this string is suffix", "prefix", "query"
                        )
                )
        );
        assertEquals(
                ImmutableSet.of(
                        "suffix of this string is suffix"
                ),
                getAllMatch(
                        new StringTermMatcher.SuffixStringTermMatcher("suffix"),
                        ImmutableSet.of(
                                "", "prefix", "s", "suffix of this string is suffix", "prefix", "query"
                        )
                )
        );
    }

    @Test
    public void testIncludeMatch() {
        assertEquals(
                ImmutableSet.of(
                        "target", "target is prefix", "contains target inside", "suffixed by target"
                ),
                getAllMatch(
                        new StringTermMatcher.IncludeStringTermMatcher("target"),
                        ImmutableSet.of(
                                "", "whatever", "suffixed by target", "target is prefix", "contains target inside", "target"
                        )
                )
        );
        assertEquals(
                ImmutableSet.of(
                        "target is prefix", "contains target inside", "suffixed by target", "target target target"
                ),
                getAllMatch(
                        new StringTermMatcher.IncludeStringTermMatcher("target"),
                        ImmutableSet.of(
                                "", "whatever", "suffixed by target", "target is prefix", "contains target inside", "target target target"
                        )
                )
        );
    }
}