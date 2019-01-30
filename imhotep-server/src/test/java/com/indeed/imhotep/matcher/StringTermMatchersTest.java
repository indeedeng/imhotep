package com.indeed.imhotep.matcher;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StringTermMatchersTest {
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
        public int commonPrefixLengthWithPreviousLowerBound() {
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
                StringTermMatchers.forRegex(".*"),
                instanceOf(AllMatchStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("foobar"),
                instanceOf(ExactStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("foo.*"),
                instanceOf(PrefixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*foo"),
                instanceOf(SuffixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*foo.*"),
                instanceOf(IncludeStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("f oo.*"),
                instanceOf(PrefixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*foo "),
                instanceOf(SuffixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*f oo.*"),
                instanceOf(IncludeStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*" + Character.MIN_LOW_SURROGATE), // stray low surrogate
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*" + Character.MIN_HIGH_SURROGATE), // stray high surrogate
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*" + Character.MIN_LOW_SURROGATE + Character.MIN_HIGH_SURROGATE), // misordered surrogate pair
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*" + Character.MIN_HIGH_SURROGATE + Character.MIN_LOW_SURROGATE), // Correct surrogate pair
                instanceOf(SuffixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("\u307b\u3052"),
                instanceOf(ExactStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("\u307b\u3052.*"),
                instanceOf(PrefixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*\u307b\u3052"),
                instanceOf(SuffixStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*\u307b\u3052.*"),
                instanceOf(IncludeStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*foo|bar.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("[f]oobar.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("a.*b"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("f.o.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex(".*.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("fo+.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("fo{1,2}.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("foo?.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("foo&bar.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("foo~a.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("[.].*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
        assertThat(
                StringTermMatchers.forRegex("<1-3>.*"),
                instanceOf(AutomatonStringTermMatcher.class)
        );
    }

    @Test
    public void testAllMatch() {
        validateMatcher(
                ImmutableSet.of("foo", "bar", "\u307b\u3052"),
                new AllMatchStringTermMatcher(),
                ImmutableSet.of("foo", "bar", "\u307b\u3052")
        );
    }

    @Test
    public void testExactMatch() {
        validateMatcher(
                ImmutableSet.of("exact"),
                new ExactStringTermMatcher("exact"),
                ImmutableSet.of("foo", "bar", "exact")
        );
        validateMatcher(
                ImmutableSet.of("\u307b\u3052"),
                new ExactStringTermMatcher("\u307b\u3052"),
                ImmutableSet.of("foo", "bar", "\u307b\u3052")
        );
    }

    @Test
    public void testPrefixMatch() {
        validateMatcher(
                ImmutableSet.of("prefix", "prefixed"),
                new PrefixStringTermMatcher("prefix"),
                ImmutableSet.of("", "p", "prefix", "prefixed", "prepared", "query", "this is a prefix")
        );
        validateMatcher(
                ImmutableSet.of("prefixed", "prefixed by prefix"),
                new PrefixStringTermMatcher("prefix"),
                ImmutableSet.of("", "p", "prefixed", "prefixed by prefix", "prepared", "query", "this is a prefix")
        );
        validateMatcher(
                ImmutableSet.of(),
                new PrefixStringTermMatcher("prefix"),
                ImmutableSet.of("", "p", "prop")
        );
        validateMatcher(
                ImmutableSet.of("prefix", "prefixed"),
                new PrefixStringTermMatcher("prefix"),
                ImmutableSet.of("", "p", "prefix", "prefixed")
        );
    }

    @Test
    public void testSuffixMatch() {
        validateMatcher(
                ImmutableSet.of("suffix", "suffix of this string is suffix"),
                new SuffixStringTermMatcher("suffix"),
                ImmutableSet.of("", "prefix", "s", "suffix", "suffix of this string is suffix", "prefix", "query")
        );
        validateMatcher(
                ImmutableSet.of("suffix of this string is suffix"),
                new SuffixStringTermMatcher("suffix"),
                ImmutableSet.of("", "prefix", "s", "suffix of this string is suffix", "prefix", "query")
        );
    }

    @Test
    public void testIncludeMatch() {
        validateMatcher(
                ImmutableSet.of("target", "target is prefix", "contains target inside", "suffixed by target"),
                new IncludeStringTermMatcher("target"),
                ImmutableSet.of("", "whatever", "suffixed by target", "target is prefix", "contains target inside", "target")
        );
        validateMatcher(
                ImmutableSet.of("target is prefix", "contains target inside", "suffixed by target", "target target target"),
                new IncludeStringTermMatcher("target"),
                ImmutableSet.of("", "whatever", "suffixed by target", "target is prefix", "contains target inside", "target target target")
        );
    }

    @Test
    public void testRegexMatch() {
        validateMatcher(
                ImmutableSet.of("foobar", "foofoobarbar"),
                new AutomatonStringTermMatcher("foo.*bar"),
                ImmutableSet.of("foobar", "fizzbuzz", "foofoobarbar")
        );
    }
}
