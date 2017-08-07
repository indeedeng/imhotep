package com.indeed.flamdex.reader;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.util.core.Pair;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author michihiko
 */
public class TestGenericStringToIntTermIterator {
    private static class DirectStringTermIterator implements StringTermIterator {
        private final List<Pair<String, Integer>> terms;
        private Pair<String, Integer> currentTerm;
        private PeekingIterator<Pair<String, Integer>> iterator;

        private DirectStringTermIterator(final List<Pair<String, Integer>> terms) {
            this.terms = Ordering.from(new Pair.HalfPairComparator()).sortedCopy(terms);
            this.iterator = Iterators.peekingIterator(terms.iterator());
        }

        @Override
        public boolean next() {
            if (iterator.hasNext()) {
                currentTerm = iterator.next();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public int docFreq() {
            return currentTerm.getSecond();
        }

        @Override
        public void close() {
        }

        @Override
        public void reset(final String term) {
            iterator = Iterators.peekingIterator(terms.iterator());
            while (iterator.hasNext() && term.compareTo(iterator.peek().getFirst()) > 0) {
                currentTerm = iterator.next();
            }
        }

        @Override
        public String term() {
            return currentTerm.getFirst();
        }
    }

    private GenericStringToIntTermIterator<DirectStringTermIterator> createStringToIntTermIterator(final List<Pair<String, Integer>> strTerms) {
        return new GenericStringToIntTermIterator<>(
                new DirectStringTermIterator(strTerms),
                new Supplier<DirectStringTermIterator>() {
                    @Override
                    public DirectStringTermIterator get() {
                        return new DirectStringTermIterator(strTerms);
                    }
                }
        );
    }

    @Test
    public void testStringToIntTermIterator() {
        final List<Pair<String, Integer>> strTerms = ImmutableList.of(
                Pair.of("1", 1),
                Pair.of("2", 2),
                Pair.of("2189", 2189),
                Pair.of("128931", 128931)
        );
        final IntTermIterator iterator = createStringToIntTermIterator(strTerms);

        assertTrue(iterator.next());
        assertEquals(1, iterator.term());
        assertTrue(iterator.next());
        assertEquals(2, iterator.term());
        assertTrue(iterator.next());
        assertEquals(2189, iterator.term());
        assertTrue(iterator.next());
        assertEquals(128931, iterator.term());
        assertFalse(iterator.next());

        iterator.reset(128932);
        assertFalse(iterator.next());

        iterator.reset(128931);
        assertTrue(iterator.next());
        assertEquals(128931, iterator.term());

        iterator.reset(3);
        assertTrue(iterator.next());
        assertEquals(2189, iterator.term());

        iterator.reset(2);
        assertTrue(iterator.next());
        assertEquals(2, iterator.term());

        iterator.reset(1);
        assertTrue(iterator.next());
        assertEquals(1, iterator.term());

        iterator.reset(0);
        assertTrue(iterator.next());
        assertEquals(1, iterator.term());
    }
}
