/*
 * Copyright (C) 2014 Indeed Inc.
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
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.InputStreamFTGSIterator;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.RawFTGSMerger;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.util.core.Pair;
import com.indeed.util.io.Files;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author jwolfe
 */
public class TestNativeFlamdexFTGSIterator {
    private static final String INT_ITERATION_FIELD = "iterationField";
    private static final String STRING_ITERATION_FIELD = "stringIterationField";
    private static final String METRIC_FIELD = "metricField";
    // this field is silly and exists for regrouping purposes.
    private static final String DOCID_FIELD = "docIdField";

    enum BitsetOptimizationLevel {
        DONT_OPTIMIZE,
        OPTIMIZE,
    }

    class ClusterSimulator {
        public static final int PORT = 8137;
        private ServerSocket serverSocket;
        private int nSplits;
        private int nStats;

        ClusterSimulator(int nSplits, int nStats) throws IOException {
            this.nSplits = nSplits;
            this.nStats = nStats;
            this.serverSocket = new ServerSocket(PORT);
        }

        public RawFTGSIterator getIterator() throws IOException {
            List<RawFTGSIterator> iters = new ArrayList<>();
            for (int i = 0; i < nSplits; i++) {
                Socket sock = new Socket("localhost", PORT);
                InputStream is = sock.getInputStream();
                RawFTGSIterator ftgs = new InputStreamFTGSIterator(is, nStats);
                iters.add(ftgs);
            }
            RawFTGSMerger merger  = new RawFTGSMerger(iters, nStats, null);
            return merger;
        }

        public void simulateRequests(RunnableFactory factory) throws IOException {
            for (int i = 0; i < nSplits; i++) {
                Socket sock = serverSocket.accept();
                Thread t = new Thread(factory.newRunnable(sock, i));
                t.start();
            }
        }

    }

    public interface RunnableFactory {
        public Runnable newRunnable(Socket socket, int i);
    }

    @Test
    public void testSimpleIteration() throws ImhotepOutOfMemoryException, IOException {
        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            MTImhotepLocalMultiSession session = makeTestSession(level);
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
    public void testSkippingField() throws ImhotepOutOfMemoryException, IOException {
        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            MTImhotepLocalMultiSession session = makeTestSession(level);
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
    public void testMultipleStats() throws ImhotepOutOfMemoryException, IOException {
        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
            MTImhotepLocalMultiSession session = makeTestSession(level);
            session.pushStat("count()");
            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{});
            try {
                ftgsIterator.nextField();
                expectTerms(Arrays.asList(new IntTerm(Integer.MIN_VALUE,
                                                      Arrays.asList(Pair.of(2, new long[]{1, 3}))),
                                          new IntTerm(-1,
                                                      Arrays.asList(Pair.of(1, new long[]{11, 3}))),
                                          new IntTerm(0,
                                                      Arrays.asList(Pair.of(1, new long[]{0, 1}),
                                                                    Pair.of(2, new long[]{0, 2}))),
                                          new IntTerm(1,
                                                      Arrays.asList(Pair.of(1, new long[]{11, 3}))),
                                          new IntTerm(Integer.MAX_VALUE,
                                                      Arrays.asList(Pair.of(2, new long[]{1, 3})))),
                            ftgsIterator);
            } finally {
                ftgsIterator.close();
                session.close();
            }
        }
    }

    private MTImhotepLocalMultiSession makeTestSession(BitsetOptimizationLevel level) throws ImhotepOutOfMemoryException, IOException {
        SimpleFlamdexReader r = makeTestFlamdexReader();
        final MTImhotepLocalMultiSession mtSession;
        final ImhotepLocalSession localSession;
        localSession = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
        final ExecutorService executor = Executors.newCachedThreadPool();
        AtomicLong foo = new AtomicLong(100000);
        mtSession = new MTImhotepLocalMultiSession(new ImhotepLocalSession[]{localSession},
                                                   new MemoryReservationContext(new ImhotepMemoryPool(
                                                           Long.MAX_VALUE)),
                                                   executor,
                                                   foo,
                                                   true);

        mtSession.regroup(new GroupRemapRule[]{new GroupRemapRule(1,
                                                                  new RegroupCondition(DOCID_FIELD,
                                                                                       true,
                                                                                       4,
                                                                                       null,
                                                                                       true),
                                                                  2,
                                                                  1)});
        mtSession.pushStat(METRIC_FIELD);
        return mtSession;
    }

    private SimpleFlamdexReader makeTestFlamdexReader() throws IOException {
        final String dir = Files.getTempDirectory("flamdex-test", "foo");

        SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, 20000L, true);

        IntFieldWriter ifw = w.getIntFieldWriter(INT_ITERATION_FIELD);
        addDataToIntTerm(ifw, Integer.MIN_VALUE, 5, 7, 8);
        addDataToIntTerm(ifw, -1, 1, 2, 3);
        addDataToIntTerm(ifw, 0, 4, 8, 9);
        addDataToIntTerm(ifw, 1, 0, 1, 2);
        addDataToIntTerm(ifw, Integer.MAX_VALUE, 5, 7, 8);
        ifw.close();

        ifw = w.getIntFieldWriter(DOCID_FIELD);
        addDataToIntTerm(ifw, 0, 0);
        addDataToIntTerm(ifw, 1, 1);
        addDataToIntTerm(ifw, 2, 2);
        addDataToIntTerm(ifw, 3, 3);
        addDataToIntTerm(ifw, 4, 4);
        addDataToIntTerm(ifw, 5, 5);
        addDataToIntTerm(ifw, 6, 6);
        addDataToIntTerm(ifw, 7, 7);
        addDataToIntTerm(ifw, 8, 8);
        addDataToIntTerm(ifw, 9, 9);
        ifw.close();

        ifw = w.getIntFieldWriter(METRIC_FIELD);
        addDataToIntTerm(ifw, 0, 4, 7, 8, 9);
        addDataToIntTerm(ifw, 1, 2, 5, 6);
        addDataToIntTerm(ifw, 5, 0, 1, 3);
        ifw.close();

        StringFieldWriter sfw = w.getStringFieldWriter(STRING_ITERATION_FIELD);
        addDataToStringTerm(sfw, "", 1, 4, 9);
        addDataToStringTerm(sfw, "english", 1, 2, 3);
        addDataToStringTerm(sfw, "日本語", 4, 5, 6);
        sfw.close();

        w.close();

        SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
        return r;
    }

    private static void addDataToStringTerm(StringFieldWriter sfw, String term, final int... docs) throws IOException {
        sfw.nextTerm(term);
        for (int doc : docs) {
            sfw.nextDoc(doc);
        }
    }

    private static void addDataToIntTerm(IntFieldWriter ifw, long term, final int... docs) throws IOException {
        ifw.nextTerm(term);
        for (int doc : docs) {
            ifw.nextDoc(doc);
        }
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
