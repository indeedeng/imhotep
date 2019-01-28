package com.indeed.imhotep.local;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import com.indeed.flamdex.api.StringTermIterator;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StringTermMatcherTest {
    private static class MockStringTermIterator implements StringTermIterator {
        private final Function<Integer, Integer> lcpTransform;
        private final List<String> terms;
        private int currentPos = -1;

        private MockStringTermIterator(final Function<Integer, Integer> lcpTransform, final Collection<String> terms) {
            this.lcpTransform = lcpTransform;
            this.terms = terms.stream()
                    .sorted(
                            Comparator.comparing(
                                    str -> str.getBytes(Charsets.UTF_8),
                                    UnsignedBytes.lexicographicalComparator()
                            )
                    )
                    .collect(Collectors.toList());
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
            if (currentPos == 0) {
                return lcpTransform.apply(0);
            } else {
                final byte[] previous = terms.get(currentPos - 1).getBytes(Charsets.UTF_8);
                final byte[] current = termStringBytes();
                for (int i = 0; (i < previous.length) && (i < current.length); ++i) {
                    if (current[i] != previous[i]) {
                        return lcpTransform.apply(i);
                    }
                }
                return lcpTransform.apply(previous.length);
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

    private static Set<String> runMatcher(final StringTermMatcher stringTermMatcher, final StringTermIterator iterator) {
        final Set<String> result = new HashSet<>();
        stringTermMatcher.run(iterator, it -> result.add(it.term()));
        return result;
    }

    private static void validateMatcher(final Set<String> expected, final StringTermMatcher stringTermMatcher, final Set<String> terms) {
        assertEquals(expected, terms.stream().filter(stringTermMatcher::matches).collect(Collectors.toSet()));
        try (final MockStringTermIterator iterator = new MockStringTermIterator(Function.identity(), terms)) {
            assertEquals(expected, runMatcher(stringTermMatcher, iterator));
        }
        try (final MockStringTermIterator iterator = new MockStringTermIterator(ignored -> 0, terms)) {
            assertEquals(expected, runMatcher(stringTermMatcher, iterator));
        }
        try (final MockStringTermIterator iterator = new MockStringTermIterator(x -> x / 2, terms)) {
            assertEquals(expected, runMatcher(stringTermMatcher, iterator));
        }
        final Random random = new Random(0);
        try (final MockStringTermIterator iterator = new MockStringTermIterator(x -> random.nextInt(x + 1), terms)) {
            assertEquals(expected, runMatcher(stringTermMatcher, iterator));
        }
    }

    @Test
    public void testAlterImplementation() {
        assertThat(
                StringTermMatcher.forRegex(".*"),
                instanceOf(StringTermMatcher.AllMatchStringTermMatcher.class)
        );
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
                StringTermMatcher.forRegex("f oo.*"),
                instanceOf(StringTermMatcher.PrefixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*foo "),
                instanceOf(StringTermMatcher.SuffixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*f oo.*"),
                instanceOf(StringTermMatcher.IncludeStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*" + Character.MIN_LOW_SURROGATE), // stray low surrogate
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*" + Character.MIN_HIGH_SURROGATE), // stray high surrogate
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*" + Character.MIN_LOW_SURROGATE + Character.MIN_HIGH_SURROGATE), // misordered surrogate pair
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*" + Character.MIN_HIGH_SURROGATE + Character.MIN_LOW_SURROGATE), // Correct surrogate pair
                instanceOf(StringTermMatcher.SuffixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("\u307b\u3052.*"),
                instanceOf(StringTermMatcher.PrefixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*\u307b\u3052"),
                instanceOf(StringTermMatcher.SuffixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*\u307b\u3052.*"),
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
                StringTermMatcher.forRegex("a.*b"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex("f.o.*"),
                instanceOf(StringTermMatcher.AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatcher.forRegex(".*.*"),
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
    public void testAllMatch() {
        validateMatcher(
                ImmutableSet.of("foo", "bar", "\u307b\u3052"),
                new StringTermMatcher.AllMatchStringTermMatcher(),
                ImmutableSet.of("foo", "bar", "\u307b\u3052")
        );
    }

    @Test
    public void testPrefixMatch() {
        validateMatcher(
                ImmutableSet.of("prefix", "prefixed"),
                new StringTermMatcher.PrefixStringTermMatcher("prefix"),
                ImmutableSet.of("", "p", "prefix", "prefixed", "prepared", "query", "this is a prefix")
        );
        validateMatcher(
                ImmutableSet.of("prefixed", "prefixed by prefix"),
                new StringTermMatcher.PrefixStringTermMatcher("prefix"),
                ImmutableSet.of("", "p", "prefixed", "prefixed by prefix", "prepared", "query", "this is a prefix")
        );
    }

    @Test
    public void testSuffixMatch() {
        validateMatcher(
                ImmutableSet.of("suffix", "suffix of this string is suffix"),
                new StringTermMatcher.SuffixStringTermMatcher("suffix"),
                ImmutableSet.of("", "prefix", "s", "suffix", "suffix of this string is suffix", "prefix", "query")
        );
        validateMatcher(
                ImmutableSet.of("suffix of this string is suffix"),
                new StringTermMatcher.SuffixStringTermMatcher("suffix"),
                ImmutableSet.of("", "prefix", "s", "suffix of this string is suffix", "prefix", "query")
        );
    }

    @Test
    public void testIncludeMatch() {
        validateMatcher(
                ImmutableSet.of("target", "target is prefix", "contains target inside", "suffixed by target"),
                new StringTermMatcher.IncludeStringTermMatcher("target"),
                ImmutableSet.of("", "whatever", "suffixed by target", "target is prefix", "contains target inside", "target")
        );
        validateMatcher(
                ImmutableSet.of("target is prefix", "contains target inside", "suffixed by target", "target target target"),
                new StringTermMatcher.IncludeStringTermMatcher("target"),
                ImmutableSet.of("", "whatever", "suffixed by target", "target is prefix", "contains target inside", "target target target")
        );
    }
}
