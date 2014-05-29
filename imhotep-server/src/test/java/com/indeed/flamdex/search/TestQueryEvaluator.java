package com.indeed.flamdex.search;

import com.google.common.collect.Sets;
import com.indeed.flamdex.MakeAFlamdex;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;
import com.indeed.flamdex.datastruct.MockFastBitSetPooler;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.reader.MockFlamdexReader;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestQueryEvaluator {
    private FlamdexReader r;
    private FastBitSet bitSet;
    private FastBitSetPooler pooler;

    @Before
    public void setUp() throws Exception {
        r = MakeAFlamdex.make();
        bitSet = new FastBitSet(r.getNumDocs());
        pooler = new MockFastBitSetPooler();
    }

    @Test
    public void testNotIndexed() throws FlamdexOutOfMemoryException  {
        final QueryEvaluator evaluator = new TermQueryEvaluator(new Term("if3", true, 999999, "")); // term not in index
        bitSet.setAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());

        evaluator.or(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());

        bitSet.setAll();
        assertEquals(bitSet.size(), bitSet.cardinality());
        evaluator.or(r, bitSet, pooler);
        assertEquals(bitSet.size(), bitSet.cardinality());

        evaluator.not(r, bitSet, pooler);
        assertEquals(bitSet.size(), bitSet.cardinality());

        bitSet.clearAll();
        evaluator.not(r, bitSet, pooler);
        assertEquals(bitSet.size(), bitSet.cardinality());
    }

    @Test
    public void testNotIndexedBoolean() throws FlamdexOutOfMemoryException {
        // if3:9999 -if3:999999
        // if3:9999 matches 10, 11, 12, 13, 14
        // if3:999999 matches nothing
        final QueryEvaluator evaluator = new BooleanQueryEvaluator(BooleanOp.AND, Arrays.asList(
                new TermQueryEvaluator(new Term("if3", true, 9999, "")),
                new BooleanQueryEvaluator(BooleanOp.NOT, Arrays.asList(new TermQueryEvaluator(new Term("if3", true, 999999, ""))))
        ));

        bitSet.setAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(5, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (Arrays.asList(10, 11, 12, 13, 14).contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
    }

    @Test
    public void testTermQuery() throws FlamdexOutOfMemoryException {
        final QueryEvaluator evaluator = new TermQueryEvaluator(new Term("if3", true, 9999, null));
        bitSet.clearAll();
        evaluator.or(r, bitSet, pooler);
        assertEquals(5, bitSet.cardinality());
        for (int i = 10; i < 15; ++i) {
            assertTrue(bitSet.get(i));
        }
        evaluator.and(r, bitSet, pooler);
        assertEquals(5, bitSet.cardinality());
        bitSet.clearAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
        evaluator.not(r, bitSet, pooler);
        assertEquals(15, bitSet.cardinality());
        for (int i = 0; i < 10; ++i) {
            assertTrue(bitSet.get(i));
        }
        for (int i = 10; i < 15; ++i) {
            assertFalse(bitSet.get(i));
        }
        for (int i = 15; i < 20; ++i) {
            assertTrue(bitSet.get(i));
        }

        final QueryEvaluator evaluator2 = new TermQueryEvaluator(new Term("sf3", false, 0, "hmmmm"));
        bitSet.clearAll();
        evaluator2.or(r, bitSet, pooler);
        assertEquals(3, bitSet.cardinality());
        assertTrue(bitSet.get(0));
        assertTrue(bitSet.get(3));
        assertTrue(bitSet.get(18));
        evaluator2.not(r, bitSet, pooler);
        assertEquals(17, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (i != 0 && i != 3 && i != 18) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
    }

    @Test
    public void testAndQuery() throws FlamdexOutOfMemoryException {
        final QueryEvaluator evaluator = new BooleanQueryEvaluator(BooleanOp.AND, Arrays.asList(
                new TermQueryEvaluator(new Term("sf4", false, 0, "asdf")),
                new TermQueryEvaluator(new Term("if2", true, 0, null)),
                new TermQueryEvaluator(new Term("sf1", false, 0, "a"))
        ));
        bitSet.clearAll();
        evaluator.or(r, bitSet, pooler);
        assertEquals(1, bitSet.cardinality());
        assertTrue(bitSet.get(19));
        evaluator.and(r, bitSet, pooler);
        assertEquals(1, bitSet.cardinality());
        assertTrue(bitSet.get(19));
        evaluator.not(r, bitSet, pooler);
        assertEquals(19, bitSet.cardinality());
        for (int i = 0; i < 19; ++i) {
            assertTrue(bitSet.get(i));
        }
    }

    @Test
    public void testOrQuery() throws FlamdexOutOfMemoryException {
        final QueryEvaluator evaluator = new BooleanQueryEvaluator(BooleanOp.OR, Arrays.asList(
                new TermQueryEvaluator(new Term("sf3", false, 0, "some string")),
                new TermQueryEvaluator(new Term("sf1", false, 0, "a")),
                new TermQueryEvaluator(new Term("if2", true, 5, null)),
                new TermQueryEvaluator(new Term("if1", true, 1, null))
        ));
        bitSet.clearAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
        evaluator.or(r, bitSet, pooler);
        assertEquals(10, bitSet.cardinality());
        for (int i = 0; i < bitSet.size(); ++i) {
            if (Sets.newHashSet(0, 1, 4, 5, 6, 7, 8, 9, 16, 19).contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.and(r, bitSet, pooler);
        assertEquals(10, bitSet.cardinality());
        for (int i = 0; i < bitSet.size(); ++i) {
            if (Sets.newHashSet(0, 1, 4, 5, 6, 7, 8, 9, 16, 19).contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.not(r, bitSet, pooler);
        assertEquals(10, bitSet.cardinality());
        for (int i = 0; i < bitSet.size(); ++i) {
            if (Sets.newHashSet(0, 1, 4, 5, 6, 7, 8, 9, 16, 19).contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
    }

    @Test
    public void testNotQuery() throws FlamdexOutOfMemoryException {
        final QueryEvaluator evaluator = new BooleanQueryEvaluator(BooleanOp.NOT, Arrays.asList(
                new TermQueryEvaluator(new Term("sf1", false, 0, "hello world"))
        ));
        bitSet.clearAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
        evaluator.or(r, bitSet, pooler);
        assertEquals(17, bitSet.cardinality());
        for (int i = 0; i < bitSet.size(); ++i) {
            if (Sets.newHashSet(3, 9, 16).contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
        boolean exc = false;
        try {
            evaluator.not(r, bitSet, pooler);
        } catch (IllegalArgumentException e) {
            exc = true;
        }
        assertTrue(exc);
    }

    @Test
    public void testRangeQuery() throws FlamdexOutOfMemoryException {
        final QueryEvaluator evaluator = new IntRangeQueryEvaluator(
                new Term("if1", true, 1, null),
                new Term("if1", true, 9000, null),
                false
        );
        bitSet.clearAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
        evaluator.or(r, bitSet, pooler);
        assertEquals(13, bitSet.cardinality());
        for (int i = 0; i < r.getNumDocs(); ++i) {
            if (Sets.newHashSet(0, 1, 5, 7, 8, 9, 11, 13, 14, 15, 16, 17, 18).contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.and(r, bitSet, pooler);
        assertEquals(13, bitSet.cardinality());
        for (int i = 0; i < r.getNumDocs(); ++i) {
            if (Sets.newHashSet(0, 1, 5, 7, 8, 9, 11, 13, 14, 15, 16, 17, 18).contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.not(r, bitSet, pooler);
        assertEquals(7, bitSet.cardinality());
        for (int i = 0; i < r.getNumDocs(); ++i) {
            if (Sets.newHashSet(0, 1, 5, 7, 8, 9, 11, 13, 14, 15, 16, 17, 18).contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
    }

    @Test
    public void testRangeQueryInclusive() throws FlamdexOutOfMemoryException {
        final QueryEvaluator evaluator = new IntRangeQueryEvaluator(
                new Term("if1", true, 1, null),
                new Term("if1", true, 9000, null),
                true
        );
        bitSet.clearAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
        evaluator.or(r, bitSet, pooler);
        assertEquals(16, bitSet.cardinality());
        for (int i = 0; i < r.getNumDocs(); ++i) {
            if (Sets.newHashSet(0, 1, 3, 5, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19).contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.and(r, bitSet, pooler);
        assertEquals(16, bitSet.cardinality());
        for (int i = 0; i < r.getNumDocs(); ++i) {
            if (Sets.newHashSet(0, 1, 3, 5, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19).contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.not(r, bitSet, pooler);
        assertEquals(4, bitSet.cardinality());
        for (int i = 0; i < r.getNumDocs(); ++i) {
            if (Sets.newHashSet(0, 1, 3, 5, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19).contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
    }

    @Test
    public void testRangeQuery2() throws FlamdexOutOfMemoryException {
        final QueryEvaluator evaluator = new IntRangeQueryEvaluator(new Term("if1", true, 0, null), new Term("if1", true, 0, null), false);
        bitSet.setAll();
        assertEquals(r.getNumDocs(), bitSet.cardinality());
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
        evaluator.or(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
        evaluator.not(r, bitSet, pooler);
        assertEquals(r.getNumDocs(), bitSet.cardinality());
    }

    @Test
    public void testStringRangeQuery() throws FlamdexOutOfMemoryException {
        MockFlamdexReader r = new MockFlamdexReader(Collections.<String>emptyList(), Arrays.asList("sf1"), Collections.<String>emptyList(), 20);
        r.addStringTerm("sf1", "a", Arrays.asList(0, 5, 9, 13));
        r.addStringTerm("sf1", "aa", Arrays.asList(1, 10, 19));
        r.addStringTerm("sf1", "abasefuhawfoae-0990458v", Arrays.asList(2, 9, 18));
        r.addStringTerm("sf1", "bb", Arrays.asList(3, 4, 8, 16));
        r.addStringTerm("sf1", "c", Arrays.asList(6, 7, 11));
        r.addStringTerm("sf1", "1", Arrays.asList(12));
        r.addStringTerm("sf1", "A", Arrays.asList(14, 15, 17));

        QueryEvaluator evaluator = new StringRangeQueryEvaluator(
                new Term("sf1", false, 0, "a"),
                new Term("sf1", false, 0, "bb"),
                false
        );

        bitSet.clearAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
        bitSet.setAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(9, bitSet.cardinality());
        final List<Integer> matchingDocs = Arrays.asList(0, 1, 2, 5, 9, 10, 13, 18, 19);
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.or(r, bitSet, pooler);
        assertEquals(9, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        bitSet.setAll();
        evaluator.or(r, bitSet, pooler);
        assertEquals(bitSet.size(), bitSet.cardinality());
        evaluator.and(r, bitSet, pooler);
        assertEquals(9, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.not(r, bitSet, pooler);
        assertEquals(11, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
        bitSet.clearAll();
        evaluator.not(r, bitSet, pooler);
        assertEquals(11, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
        bitSet.setAll();
        evaluator.not(r, bitSet, pooler);
        assertEquals(11, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
        bitSet.invertAll();
        evaluator.not(r, bitSet, pooler);
        assertEquals(11, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
    }

    @Test
    public void testStringRangeQueryInclusive() throws FlamdexOutOfMemoryException {
        MockFlamdexReader r = new MockFlamdexReader(Collections.<String>emptyList(), Arrays.asList("sf1"), Collections.<String>emptyList(), 20);
        r.addStringTerm("sf1", "a", Arrays.asList(0, 5, 9, 13));
        r.addStringTerm("sf1", "aa", Arrays.asList(1, 10, 19));
        r.addStringTerm("sf1", "abasefuhawfoae-0990458v", Arrays.asList(2, 9, 18));
        r.addStringTerm("sf1", "bb", Arrays.asList(3, 4, 8, 16));
        r.addStringTerm("sf1", "c", Arrays.asList(6, 7, 11));
        r.addStringTerm("sf1", "1", Arrays.asList(12));
        r.addStringTerm("sf1", "A", Arrays.asList(14, 15, 17));

        QueryEvaluator evaluator = new StringRangeQueryEvaluator(
                new Term("sf1", false, 0, "a"),
                new Term("sf1", false, 0, "bb"),
                true
        );

        bitSet.clearAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
        bitSet.setAll();
        evaluator.and(r, bitSet, pooler);
        assertEquals(13, bitSet.cardinality());
        final List<Integer> matchingDocs = Arrays.asList(0, 1, 2, 3, 4, 5, 8, 9, 10, 13, 16, 18, 19);
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.or(r, bitSet, pooler);
        assertEquals(13, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        bitSet.setAll();
        evaluator.or(r, bitSet, pooler);
        assertEquals(bitSet.size(), bitSet.cardinality());
        evaluator.and(r, bitSet, pooler);
        assertEquals(13, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
        evaluator.not(r, bitSet, pooler);
        assertEquals(7, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
        bitSet.clearAll();
        evaluator.not(r, bitSet, pooler);
        assertEquals(7, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
        bitSet.setAll();
        evaluator.not(r, bitSet, pooler);
        assertEquals(7, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
        bitSet.invertAll();
        evaluator.not(r, bitSet, pooler);
        assertEquals(7, bitSet.cardinality());
        for (int i = 0; i < 20; ++i) {
            if (matchingDocs.contains(i)) {
                assertFalse(bitSet.get(i));
            } else {
                assertTrue(bitSet.get(i));
            }
        }
        evaluator.and(r, bitSet, pooler);
        assertEquals(0, bitSet.cardinality());
    }
}
