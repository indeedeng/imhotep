/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.local;

import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.util.core.Pair;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        for (final BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            final ImhotepLocalSession session = makeTestSession(level);
            final FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{STRING_ITERATION_FIELD});
            try {
                testExpectedIntField(ftgsIterator);
                testExpectedStringField(ftgsIterator);
                assertFalse(ftgsIterator.nextField());
            }  finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    @Test
    public void testSkippingField() throws ImhotepOutOfMemoryException {
        for (final BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            final ImhotepLocalSession session = makeTestSession(level);
            final FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{STRING_ITERATION_FIELD});
            try {
                assertTrue(ftgsIterator.nextField());
                testExpectedStringField(ftgsIterator);
                assertFalse(ftgsIterator.nextField());
            } finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    @Test
    public void testSkippingTerm() throws ImhotepOutOfMemoryException {
        for (final BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            final MockFlamdexReader r = new MockFlamdexReader();
            r.addIntTerm("if1", 0, 1, 2);
            r.addIntTerm("if1", 1, 3, 4);
            final ImhotepLocalSession session = new ImhotepJavaLocalSession("TestFlamdexFTGSIterator", r);
            session.pushStat("count()");
            final FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{"if1"}, new String[]{});

            try {
                final long[] stats = new long[1];
                ftgsIterator.nextField();
                ftgsIterator.nextTerm();
                ftgsIterator.nextTerm();
                assertEquals(1, ftgsIterator.termIntVal());
                assertTrue(ftgsIterator.nextGroup());
                assertEquals(1, ftgsIterator.group());
                ftgsIterator.groupStats(stats);
                assertArrayEquals(new long[]{2}, stats);
                assertFalse(ftgsIterator.nextTerm());
                assertFalse(ftgsIterator.nextField());
            } finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    @Test
    public void testEmptyField() throws ImhotepOutOfMemoryException {
        for (final BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            final MockFlamdexReader r = new MockFlamdexReader();
            final ImhotepLocalSession session = new ImhotepJavaLocalSession("TestFlamdexFTGSIterator", r);
            final FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{"if1"}, new String[]{"sf1"});
            try {
                assertTrue(ftgsIterator.nextField());
                assertEquals("if1", ftgsIterator.fieldName());
                assertFalse(ftgsIterator.nextTerm());
                assertTrue(ftgsIterator.nextField());
                assertEquals("sf1", ftgsIterator.fieldName());
                assertFalse(ftgsIterator.nextTerm());
            } finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    @Test
    public void testZeroStats() throws ImhotepOutOfMemoryException {
        for (final BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            final MockFlamdexReader r = new MockFlamdexReader();
            r.addIntTerm("if1", 1, 0, 1, 2);
            final ImhotepLocalSession session = new ImhotepJavaLocalSession("TestFlamdexFTGSIterator", r);
            final FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{"if1"}, new String[]{});

            try {
                final long[] emptyBuff = new long[0];
                assertTrue(ftgsIterator.nextField());
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
        for (final BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            final ImhotepLocalSession session = makeTestSession(level);
            session.pushStat("count()");
            final FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{});
            try {
                ftgsIterator.nextField();
                expectTerms(Arrays.asList(
                        new IntTerm(Integer.MIN_VALUE, Collections.singletonList(Pair.of(2, new long[]{1, 3}))),
                        new IntTerm(-1, Collections.singletonList(Pair.of(1, new long[]{11, 3}))),
                        new IntTerm(0, Arrays.asList(Pair.of(1, new long[]{0, 1}), Pair.of(2, new long[]{0, 2}))),
                        new IntTerm(1, Collections.singletonList(Pair.of(1, new long[]{11, 3}))),
                        new IntTerm(Integer.MAX_VALUE, Collections.singletonList(Pair.of(2, new long[]{1, 3})))
                ), ftgsIterator);
            } finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    private ImhotepLocalSession makeTestSession(final BitsetOptimizationLevel level) throws ImhotepOutOfMemoryException {
        final MockFlamdexReader r = makeTestFlamdexReader();
        final ImhotepLocalSession session = new ImhotepJavaLocalSession("TestFlamdexFTGSIterator", r);
        session.regroup(new GroupRemapRule[]{new GroupRemapRule(1, new RegroupCondition(DOCID_FIELD, true, 4, null, true), 2, 1)});
        session.pushStat(METRIC_FIELD);
        return session;
    }

    private MockFlamdexReader makeTestFlamdexReader() {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r = new MockFlamdexReader(
                Arrays.asList(INT_ITERATION_FIELD, METRIC_FIELD, DOCID_FIELD),
                Collections.singletonList(STRING_ITERATION_FIELD),
                Arrays.asList(INT_ITERATION_FIELD, METRIC_FIELD, DOCID_FIELD),
                10,
                shardDir
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
        final int term;
        final List<Pair<Integer, long[]>> groupStats;

        private IntTerm(final int term, final List<Pair<Integer, long[]>> groupStats) {
            this.term = term;
            this.groupStats = groupStats;
        }
    }

    private void expectTerms(final List<IntTerm> terms, final FTGSIterator ftgsIterator) {
        final long[] stats = new long[terms.get(0).groupStats.get(0).getSecond().length];
        for (final IntTerm term : terms) {
            assertTrue(ftgsIterator.nextTerm());
            assertEquals(term.term, ftgsIterator.termIntVal());
            for (final Pair<Integer, long[]> group : term.groupStats) {
                assertTrue(ftgsIterator.nextGroup());
                assertEquals((int)group.getFirst(), ftgsIterator.group());
                ftgsIterator.groupStats(stats);
                assertArrayEquals(group.getSecond(), stats);
            }
        }

    }

    private void testExpectedIntField(final FTGSIterator ftgsIterator) {
        assertTrue(ftgsIterator.nextField());
        assertEquals(INT_ITERATION_FIELD, ftgsIterator.fieldName());
        assertTrue(ftgsIterator.fieldIsIntType());
        expectTerms(Arrays.asList(
                new IntTerm(Integer.MIN_VALUE, Collections.singletonList(Pair.of(2, new long[]{1}))),
                new IntTerm(-1, Collections.singletonList(Pair.of(1, new long[]{11}))),
                new IntTerm(0, Arrays.asList(Pair.of(1, new long[]{0}), Pair.of(2, new long[]{0}))),
                new IntTerm(1, Collections.singletonList(Pair.of(1, new long[]{11}))),
                new IntTerm(Integer.MAX_VALUE, Collections.singletonList(Pair.of(2, new long[]{1})))
        ), ftgsIterator);
        assertFalse(ftgsIterator.nextGroup());
        assertFalse(ftgsIterator.nextTerm());
    }

    private void testExpectedStringField(final FTGSIterator ftgsIterator) {
        final long[] stats = new long[1];

        assertTrue(ftgsIterator.nextField());
        assertEquals(STRING_ITERATION_FIELD, ftgsIterator.fieldName());
        assertFalse(ftgsIterator.fieldIsIntType());

        assertTrue(ftgsIterator.nextTerm());
        assertEquals("", ftgsIterator.termStringVal());
        assertTrue(ftgsIterator.nextGroup());
        assertEquals(1, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{5}, stats);
        assertTrue(ftgsIterator.nextGroup());
        assertEquals(2, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{0}, stats);
        assertFalse(ftgsIterator.nextGroup());

        assertTrue(ftgsIterator.nextTerm());
        assertEquals("english", ftgsIterator.termStringVal());
        assertTrue(ftgsIterator.nextGroup());
        assertEquals(1, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{11}, stats);
        assertFalse(ftgsIterator.nextGroup());

        assertTrue(ftgsIterator.nextTerm());
        assertEquals("日本語", ftgsIterator.termStringVal());
        assertTrue(ftgsIterator.nextGroup());
        assertEquals(1, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{0}, stats);
        assertTrue(ftgsIterator.nextGroup());
        assertEquals(2, ftgsIterator.group());
        ftgsIterator.groupStats(stats);
        assertArrayEquals(new long[]{2}, stats);
        assertFalse(ftgsIterator.nextGroup());
        assertFalse(ftgsIterator.nextTerm());
    }
}
