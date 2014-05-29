package com.indeed.imhotep.local;

import com.indeed.util.core.Pair;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author jwolfe
 */
public class TestFlamdexFTGSIterator {
    private static final String INT_ITERATION_FIELD = "iterationField";
    private static final String STRING_ITERATION_FIELD = "stringIterationField";
    private static final String METRIC_FIELD = "metricField";
    // this field is silly and exists for regrouping purposes.
    private static final String DOCID_FIELD = "docIdField";

    enum BitsetOptimizationLevel {
        DONT_OPTIMIZE,
        OPTIMIZE,
    }

    @Test
    public void testSimpleIteration() throws ImhotepOutOfMemoryException {
        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            ImhotepLocalSession session = makeTestSession(level);
            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{STRING_ITERATION_FIELD});
            try {
                testExpectedIntField(ftgsIterator);
                testExpectedStringField(ftgsIterator);
                assertEquals(false, ftgsIterator.nextField());
            }  finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    @Test
    public void testSkippingField() throws ImhotepOutOfMemoryException {
        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            ImhotepLocalSession session = makeTestSession(level);
            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{STRING_ITERATION_FIELD});
            try {
                assertEquals(true, ftgsIterator.nextField());
                testExpectedStringField(ftgsIterator);
                assertEquals(false, ftgsIterator.nextField());
            } finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    @Test
    public void testSkippingTerm() throws ImhotepOutOfMemoryException {
        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            MockFlamdexReader r = new MockFlamdexReader();
            r.addIntTerm("if1", 0, 1, 2);
            r.addIntTerm("if1", 1, 3, 4);
            ImhotepLocalSession session = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
            session.pushStat("count()");
            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{"if1"}, new String[]{});

            try {
                final long[] stats = new long[1];
                ftgsIterator.nextField();
                ftgsIterator.nextTerm();
                ftgsIterator.nextTerm();
                assertEquals(1, ftgsIterator.termIntVal());
                assertEquals(true, ftgsIterator.nextGroup());
                assertEquals(1, ftgsIterator.group());
                ftgsIterator.groupStats(stats);
                assertArrayEquals(new long[]{2}, stats);
                assertEquals(false, ftgsIterator.nextTerm());
                assertEquals(false, ftgsIterator.nextField());
            } finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    @Test
    public void testEmptyField() throws ImhotepOutOfMemoryException {
        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            MockFlamdexReader r = new MockFlamdexReader();
            ImhotepLocalSession session = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{"if1"}, new String[]{"sf1"});
            try {
                assertEquals(true, ftgsIterator.nextField());
                assertEquals("if1", ftgsIterator.fieldName());
                assertEquals(false, ftgsIterator.nextTerm());
                assertEquals(true, ftgsIterator.nextField());
                assertEquals("sf1", ftgsIterator.fieldName());
                assertEquals(false, ftgsIterator.nextTerm());
            } finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    @Test
    public void testZeroStats() throws ImhotepOutOfMemoryException {
        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            MockFlamdexReader r = new MockFlamdexReader();
            r.addIntTerm("if1", 1, 0, 1, 2);
            ImhotepLocalSession session = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{"if1"}, new String[]{});

            try {
                final long[] emptyBuff = new long[0];
                assertEquals(true, ftgsIterator.nextField());
                ftgsIterator.nextTerm();
                ftgsIterator.group();
                ftgsIterator.groupStats(emptyBuff);
            } finally {
                ftgsIterator.close();
                session.close();
            }
            // Just making sure nothing goes catastrophically wrong
        }
    }

    @Test
    public void testMultipleStats() throws ImhotepOutOfMemoryException {
        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            ImhotepLocalSession session = makeTestSession(level);
            session.pushStat("count()");
            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{});
            try {
                ftgsIterator.nextField();
                expectTerms(Arrays.asList(
                        new IntTerm(Integer.MIN_VALUE, Arrays.asList(Pair.of(2, new long[]{1, 3}))),
                        new IntTerm(-1, Arrays.asList(Pair.of(1, new long[]{11, 3}))),
                        new IntTerm(0, Arrays.asList(Pair.of(1, new long[]{0, 1}), Pair.of(2, new long[]{0, 2}))),
                        new IntTerm(1, Arrays.asList(Pair.of(1, new long[]{11, 3}))),
                        new IntTerm(Integer.MAX_VALUE, Arrays.asList(Pair.of(2, new long[]{1, 3})))
                ), ftgsIterator);
            } finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    private ImhotepLocalSession makeTestSession(BitsetOptimizationLevel level) throws ImhotepOutOfMemoryException {
        MockFlamdexReader r = makeTestFlamdexReader();
        ImhotepLocalSession session = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
        session.regroup(new GroupRemapRule[]{new GroupRemapRule(1, new RegroupCondition(DOCID_FIELD, true, 4, null, true), 2, 1)});
        session.pushStat(METRIC_FIELD);
        return session;
    }

    private MockFlamdexReader makeTestFlamdexReader() {
        MockFlamdexReader r = new MockFlamdexReader(
                Arrays.asList(INT_ITERATION_FIELD, METRIC_FIELD, DOCID_FIELD),
                Arrays.<String>asList(STRING_ITERATION_FIELD),
                Arrays.asList(INT_ITERATION_FIELD, METRIC_FIELD, DOCID_FIELD),
                10
        );
        r.addIntTerm(INT_ITERATION_FIELD, Integer.MIN_VALUE, 5, 7, 8);
        r.addIntTerm(INT_ITERATION_FIELD, -1, 1, 2, 3);
        r.addIntTerm(INT_ITERATION_FIELD, 0, 4, 8, 9);
        r.addIntTerm(INT_ITERATION_FIELD, 1, 0, 1, 2);
        r.addIntTerm(INT_ITERATION_FIELD, Integer.MAX_VALUE, 5, 7, 8);
        r.addStringTerm(STRING_ITERATION_FIELD, "", 1, 4, 9);
        r.addStringTerm(STRING_ITERATION_FIELD, "english", 1, 2, 3);
        r.addStringTerm(STRING_ITERATION_FIELD, "日本語", 4, 5, 6);
        r.addIntTerm(DOCID_FIELD, 0, 0);
        r.addIntTerm(DOCID_FIELD, 1, 1);
        r.addIntTerm(DOCID_FIELD, 2, 2);
        r.addIntTerm(DOCID_FIELD, 3, 3);
        r.addIntTerm(DOCID_FIELD, 4, 4);
        r.addIntTerm(DOCID_FIELD, 5, 5);
        r.addIntTerm(DOCID_FIELD, 6, 6);
        r.addIntTerm(DOCID_FIELD, 7, 7);
        r.addIntTerm(DOCID_FIELD, 8, 8);
        r.addIntTerm(DOCID_FIELD, 9, 9);
        r.addIntTerm(METRIC_FIELD, 0, 4, 7, 8, 9);
        r.addIntTerm(METRIC_FIELD, 1, 2, 5, 6);
        r.addIntTerm(METRIC_FIELD, 5, 0, 1, 3);
        return r;
    }

    private static class IntTerm {
        int term;
        List<Pair<Integer, long[]>> groupStats;

        private IntTerm(int term, List<Pair<Integer, long[]>> groupStats) {
            this.term = term;
            this.groupStats = groupStats;
        }
    }

    private void expectTerms(List<IntTerm> terms, FTGSIterator ftgsIterator) {
        long[] stats = new long[terms.get(0).groupStats.get(0).getSecond().length];
        for (IntTerm term : terms) {
            assertEquals(true, ftgsIterator.nextTerm());
            assertEquals(term.term, ftgsIterator.termIntVal());
            for (Pair<Integer, long[]> group : term.groupStats) {
                assertEquals(true, ftgsIterator.nextGroup());
                assertEquals((int)group.getFirst(), ftgsIterator.group());
                ftgsIterator.groupStats(stats);
                assertArrayEquals(group.getSecond(), stats);
            }
        }

    }

    private void testExpectedIntField(FTGSIterator ftgsIterator) {
        assertEquals(true, ftgsIterator.nextField());
        assertEquals(INT_ITERATION_FIELD, ftgsIterator.fieldName());
        assertEquals(true, ftgsIterator.fieldIsIntType());
        expectTerms(Arrays.asList(
                new IntTerm(Integer.MIN_VALUE, Arrays.asList(Pair.of(2, new long[]{1}))),
                new IntTerm(-1, Arrays.asList(Pair.of(1, new long[]{11}))),
                new IntTerm(0, Arrays.asList(Pair.of(1, new long[]{0}), Pair.of(2, new long[]{0}))),
                new IntTerm(1, Arrays.asList(Pair.of(1, new long[]{11}))),
                new IntTerm(Integer.MAX_VALUE, Arrays.asList(Pair.of(2, new long[]{1})))
        ), ftgsIterator);
        assertEquals(false, ftgsIterator.nextGroup());
        assertEquals(false, ftgsIterator.nextTerm());
    }

    private void testExpectedStringField(FTGSIterator ftgsIterator) {
        long[] stats = new long[1];

        assertEquals(true, ftgsIterator.nextField());
        assertEquals(STRING_ITERATION_FIELD, ftgsIterator.fieldName());
        assertEquals(false, ftgsIterator.fieldIsIntType());

        assertEquals(true, ftgsIterator.nextTerm());
        assertEquals("", ftgsIterator.termStringVal());
        assertEquals(true, ftgsIterator.nextGroup());
        assertEquals(1, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{5}, stats);
        assertEquals(true, ftgsIterator.nextGroup());
        assertEquals(2, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{0}, stats);
        assertEquals(false, ftgsIterator.nextGroup());

        assertEquals(true, ftgsIterator.nextTerm());
        assertEquals("english", ftgsIterator.termStringVal());
        assertEquals(true, ftgsIterator.nextGroup());
        assertEquals(1, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{11}, stats);
        assertEquals(false, ftgsIterator.nextGroup());

        assertEquals(true, ftgsIterator.nextTerm());
        assertEquals("日本語", ftgsIterator.termStringVal());
        assertEquals(true, ftgsIterator.nextGroup());
        assertEquals(1, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{0}, stats);
        assertEquals(true, ftgsIterator.nextGroup());
        assertEquals(2, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{2}, stats);
        assertEquals(false, ftgsIterator.nextGroup());
        assertEquals(false, ftgsIterator.nextTerm());
    }
}
