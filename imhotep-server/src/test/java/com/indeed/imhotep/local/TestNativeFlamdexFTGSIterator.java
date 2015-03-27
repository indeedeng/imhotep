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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import com.indeed.util.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    public static class ClusterSimulator {
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

        public void simulateServer(final RunnableFactory factory) throws IOException {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < nSplits; i++) {
                            Socket sock = serverSocket.accept();
                            Thread t = new Thread(factory.newRunnable(sock, i));
                            t.start();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

    }

    public interface RunnableFactory {
        public Runnable newRunnable(Socket socket, int i);
    }

    public static class WriteFTGSRunner implements RunnableFactory {
        private final MTImhotepLocalMultiSession session;
        private final String[] intFields;
        private final String[] stringFields;
        private final int numSplits;

        public WriteFTGSRunner(MTImhotepLocalMultiSession session,
                               String[] intFields,
                               String[] stringFields,
                               int numSplits) {
            this.session = session;
            this.intFields = intFields;
            this.stringFields = stringFields;
            this.numSplits = numSplits;
        }

        @Override
        public Runnable newRunnable(final Socket socket, final int i) {
            return new Runnable() {
                @Override
                public void run() {
                    session.writeFTGSIteratorSplit(intFields, stringFields, i, numSplits, socket);
                    try {
                        socket.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
    }

    public static class FieldDesc {
        public String name;
        public boolean isIntfield;
        public int numDocs;
        public long termVal;
    }

    private final Random rand = new Random();
    private final int MAX_STRING_TERM_LEN = 2048;

    private void createFlamdexIntField(final String dir,
                                       final String intFieldName,
                                       final long term,
                                       final int size) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, size, true);
        IntFieldWriter ifw = w.getIntFieldWriter(intFieldName);
        List<Integer> docs = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            docs.add(i);
        }

        Map<Long, List<Integer>> map = Maps.newTreeMap();
        while (!docs.isEmpty()) {
            final List<Integer> selectedDocs;
            if (map.containsKey(term)) {
                selectedDocs = map.get(term);
            } else {
                selectedDocs = Lists.newArrayList();
            }

            final int maxDocsPerTerm = Math.min(docs.size(), maxTermDocs);
            int numDocs = docs.size() > 1 ? rand.nextInt(maxDocsPerTerm - 1) + 1 : 1;

            for (int i = 0; i < numDocs; ++i) {
                selectedDocs.add(docs.remove(rand.nextInt(docs.size())));
            }
            Collections.sort(selectedDocs);
            map.put(term, selectedDocs);
        }
        for (long t : map.keySet()) {
            ifw.nextTerm(t);
            List<Integer> selectedDocs = map.get(t);
            for (int doc : selectedDocs) {
                ifw.nextDoc(doc);
            }
        }
        ifw.close();
        w.close();
    }

    private void createFlamdexStringField(final String dir,
                                       final String stringFieldName,
                                       final int size) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, size, true);
        StringFieldWriter sfw = w.getStringFieldWriter(stringFieldName);
        List<Integer> docs = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            docs.add(i);
        }

        Map<String, List<Integer>> map = Maps.newTreeMap();
        while (!docs.isEmpty()) {
            String term = RandomStringUtils.randomAlphabetic(rand.nextInt(MAX_STRING_TERM_LEN));
            if (map.containsKey(term))
                continue;
            final int maxDocsPerTerm = Math.min(docs.size(), maxTermDocs);
            int numDocs = docs.size() > 1 ? rand.nextInt(maxDocsPerTerm - 1) + 1 : 1;
            List<Integer> selectedDocs = Lists.newArrayList();
            for (int i = 0; i < numDocs; ++i) {
                selectedDocs.add(docs.remove(rand.nextInt(docs.size())));
            }
            Collections.sort(selectedDocs);
            map.put(term, selectedDocs);
        }
        for (String term : map.keySet()) {
            sfw.nextTerm(term);
            List<Integer> selectedDocs = map.get(term);
            for (int doc : selectedDocs) {
                sfw.nextDoc(doc);
            }
        }
        sfw.close();
        w.close();
    }

    private String generateShard(List<FieldDesc> fieldDescs) throws IOException {
        final String dir = Files.getTempDirectory("native-ftgs-test", "fubar");

        for (FieldDesc fd : fieldDescs) {
            if (fd.isIntfield) {
                createFlamdexIntField(dir, fd.name, fd.termVal, fd.numDocs);
            } else {
                createFlamdexStringField(dir, fd.name, fd.numDocs);
            }
        }

        return dir;
    }

    private String copyShard(String dir) throws IOException {
        final String new_dir = Files.getTempDirectory("native-ftgs-test", "fubar");

        FileUtils.copyDirectory(new File(dir), new File(new_dir));

        return new_dir;
    }

    public enum MetricMaxSizes {
        LONG(Long.MAX_VALUE, Long.MIN_VALUE, "long"),
        INT(Integer.MAX_VALUE, Integer.MIN_VALUE, "int"),
        CHAR(65535, 0, "unsigned short"),
        SHORT(Short.MAX_VALUE, Short.MIN_VALUE, "short"),
        SIGNED_BYTE(Byte.MAX_VALUE, Byte.MIN_VALUE, "signed byte"),
        BYTE(255,0,"unsigned byte"),
        BINARY(1, 0, "binary");

        private final long maxVal;
        private final long minVal;
        private final String name;
        private final Random foo = new Random();

        MetricMaxSizes(long maxVal, long minVal, String name) {
            this.maxVal = maxVal;
            this.minVal = minVal;
            this.name = name;
        }

        public long getMaxVal() {
            return maxVal;
        }

        public long getMinVal() {
            return minVal;
        }

        public String getName() {
            return name;
        }

        public long randVal() {
            return (this.foo.nextLong() % (this.maxVal - this.minVal)) + this.minVal;
        }

        public static MetricMaxSizes getRandomSize() {
            switch (LONG.foo.nextInt(7)) {
                case 0:
                    return LONG;
                case 1:
                    return INT;
                case 2:
                    return CHAR;
                case 3:
                    return SHORT;
                case 4:
                    return SIGNED_BYTE;
                case 5:
                    return BYTE;
                case 6:
                    return BINARY;
                default:
                    throw new UnsupportedOperationException("Wha?!");
            }
        }
    }

    private static final int MAX_N_METRICS = 64;
    private static final int MAX_FIELD_NAME_LEN = 32;


    private MTImhotepLocalMultiSession makeTestSession(List<String> shardDirs,
                                                       List<FieldDesc> metricFieldDescs,
                                                       BitsetOptimizationLevel level) throws ImhotepOutOfMemoryException, IOException {
        ImhotepLocalSession[] localSessions = new ImhotepLocalSession[shardDirs.size()];
        for (int i = 0; i < shardDirs.size(); i++) {
            final String dir = shardDirs.get(i);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            final ImhotepLocalSession localSession;
            localSession = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
            localSessions[i] = localSession;
        }

        final ExecutorService executor = Executors.newCachedThreadPool();
        AtomicLong foo = new AtomicLong(100000);
        final MTImhotepLocalMultiSession mtSession;
        mtSession = new MTImhotepLocalMultiSession(localSessions,
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
        for (FieldDesc fd : metricFieldDescs) {
            mtSession.pushStat(fd.name);
        }
        return mtSession;
    }

    @Test
    public void testSingleShard() throws IOException, ImhotepOutOfMemoryException {
        // need at least one
        final int nMetrics = rand.nextInt(MAX_N_METRICS - 1) + 1;
        final List<FieldDesc> fieldDescs = new ArrayList<>(nMetrics);
        String fieldName;

        // create field
        {
            final FieldDesc fd = new FieldDesc();
            fd.isIntfield = false;
            fd.name = RandomStringUtils.randomAlphabetic(MAX_FIELD_NAME_LEN);
            fieldName = fd.name;
            fd.numDocs = rand.nextInt(64 * 2 ^ 20);  // 64MB
            fieldDescs.add(fd);
        }

        // create metrics
        for (int i = 0; i < nMetrics; i++) {
            final FieldDesc fd = new FieldDesc();
            fd.isIntfield = true;
            fd.name = RandomStringUtils.randomAlphabetic(MAX_FIELD_NAME_LEN);
            fd.termVal = MetricMaxSizes.getRandomSize().randVal();
            fd.numDocs = rand.nextInt(64 * 2^20);  // 64MB
            fieldDescs.add(fd);
        }

        String shardname = generateShard(fieldDescs);
        String shardCopy = copyShard(shardname);

        // remove string field, leaving metric fields
        fieldDescs.remove(0);

        final String[] stringFields = new String[]{fieldName};
        final String[] intFields = new String[0];

        FTGSIterator verificationIter = getVerificationIterator(Arrays.asList(shardCopy),
                                                                fieldDescs,
                                                                BitsetOptimizationLevel.OPTIMIZE,
                                                                stringFields,
                                                                intFields);

        final MTImhotepLocalMultiSession session;
        session = makeTestSession(Arrays.asList(shardname),
                                  fieldDescs,
                                  BitsetOptimizationLevel.OPTIMIZE);

        RunnableFactory factory =
                new WriteFTGSRunner(session, stringFields, intFields, 8);
        ClusterSimulator simulator = new ClusterSimulator(8, fieldDescs.size());

        simulator.simulateServer(factory);
        RawFTGSIterator ftgsIterator = simulator.getIterator();

        compareIterators(ftgsIterator, verificationIter, nMetrics);
    }

    private void compareIterators(FTGSIterator test, FTGSIterator valid, int numStats) {
        final long[] validStats = new long[numStats];
        final long[] testStats = new long[numStats];

        while (valid.nextField()) {
            assertTrue(test.nextField());
            assertEquals(valid.fieldName(), test.fieldName());
            while (valid.nextTerm()) {
                assertTrue(test.nextTerm());
                assertEquals(valid.fieldIsIntType(), test.fieldIsIntType());
                if (valid.fieldIsIntType()) {
                    assertEquals(valid.termIntVal(), test.termIntVal());
                } else {
                    assertEquals(valid.termStringVal(), test.termStringVal());
                }
                while (valid.nextGroup()) {
                    assertTrue(test.nextGroup());
                    assertEquals(valid.group(), test.group());

                    valid.groupStats(validStats);
                    test.groupStats(testStats);
                    assertArrayEquals(validStats,testStats);
                }
                assertFalse(test.nextGroup());
            }
            assertFalse(test.nextTerm());
        }
        assertFalse(test.nextField());
    }

    private FTGSIterator getVerificationIterator(List<String> shardDirs,
                                                 List<FieldDesc> metricFieldDescs,
                                                 BitsetOptimizationLevel level,
                                                 String[] stringFields,
                                                 String[] intFields) throws ImhotepOutOfMemoryException, IOException {
        ImhotepLocalSession[] localSessions = new ImhotepLocalSession[shardDirs.size()];
        for (int i = 0; i < shardDirs.size(); i++) {
            final String dir = shardDirs.get(i);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            final ImhotepLocalSession localSession;
            localSession = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
            localSessions[i] = localSession;
        }

        final ExecutorService executor = Executors.newCachedThreadPool();
        AtomicLong foo = new AtomicLong(100000);
        final MTImhotepLocalMultiSession mtSession;
        mtSession = new MTImhotepLocalMultiSession(localSessions,
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
        for (FieldDesc fd : metricFieldDescs) {
            mtSession.pushStat(fd.name);
        }

        return mtSession.getFTGSIterator(stringFields, intFields);
    }

//    @Test
//    public void testSimpleIteration() throws ImhotepOutOfMemoryException, IOException {
//        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
//            MTImhotepLocalMultiSession session = makeTestSession(level);
//            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{STRING_ITERATION_FIELD});
//            try {
//                testExpectedIntField(ftgsIterator);
//                testExpectedStringField(ftgsIterator);
//                assertEquals(false, ftgsIterator.nextField());
//            }  finally {
//                ftgsIterator.close();
//                session.close();
//            }
//        }
//    }
//
//    @Test
//    public void testSkippingField() throws ImhotepOutOfMemoryException, IOException {
//        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
//            MTImhotepLocalMultiSession session = makeTestSession(level);
//            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{STRING_ITERATION_FIELD});
//            try {
//                assertEquals(true, ftgsIterator.nextField());
//                testExpectedStringField(ftgsIterator);
//                assertEquals(false, ftgsIterator.nextField());
//            } finally {
//                ftgsIterator.close();
//                session.close();
//            }
//        }
//    }
//
//    @Test
//    public void testSkippingTerm() throws ImhotepOutOfMemoryException {
//        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
//            MockFlamdexReader r = new MockFlamdexReader();
//            r.addIntTerm("if1", 0, 1, 2);
//            r.addIntTerm("if1", 1, 3, 4);
//            ImhotepLocalSession session = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
//            session.pushStat("count()");
//            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{"if1"}, new String[]{});
//
//            try {
//                final long[] stats = new long[1];
//                ftgsIterator.nextField();
//                ftgsIterator.nextTerm();
//                ftgsIterator.nextTerm();
//                assertEquals(1, ftgsIterator.termIntVal());
//                assertEquals(true, ftgsIterator.nextGroup());
//                assertEquals(1, ftgsIterator.group());
//                ftgsIterator.groupStats(stats);
//                assertArrayEquals(new long[]{2}, stats);
//                assertEquals(false, ftgsIterator.nextTerm());
//                assertEquals(false, ftgsIterator.nextField());
//            } finally {
//                ftgsIterator.close();
//                session.close();
//            }
//        }
//    }
//
//    @Test
//    public void testEmptyField() throws ImhotepOutOfMemoryException {
//        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
//            MockFlamdexReader r = new MockFlamdexReader();
//            ImhotepLocalSession session = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
//            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{"if1"}, new String[]{"sf1"});
//            try {
//                assertEquals(true, ftgsIterator.nextField());
//                assertEquals("if1", ftgsIterator.fieldName());
//                assertEquals(false, ftgsIterator.nextTerm());
//                assertEquals(true, ftgsIterator.nextField());
//                assertEquals("sf1", ftgsIterator.fieldName());
//                assertEquals(false, ftgsIterator.nextTerm());
//            } finally {
//                ftgsIterator.close();
//                session.close();
//            }
//        }
//    }
//
//    @Test
//    public void testZeroStats() throws ImhotepOutOfMemoryException {
//        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
//            MockFlamdexReader r = new MockFlamdexReader();
//            r.addIntTerm("if1", 1, 0, 1, 2);
//            ImhotepLocalSession session = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
//            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{"if1"}, new String[]{});
//
//            try {
//                final long[] emptyBuff = new long[0];
//                assertEquals(true, ftgsIterator.nextField());
//                ftgsIterator.nextTerm();
//                ftgsIterator.group();
//                ftgsIterator.groupStats(emptyBuff);
//            } finally {
//                ftgsIterator.close();
//                session.close();
//            }
//            // Just making sure nothing goes catastrophically wrong
//        }
//    }
//
//    @Test
//    public void testMultipleStats() throws ImhotepOutOfMemoryException, IOException {
//        for (BitsetOptimizationLevel level : BitsetOptimizationLevel.values()) {
//            MTImhotepLocalMultiSession session = makeTestSession(level);
//            session.pushStat("count()");
//            FTGSIterator ftgsIterator = session.getFTGSIterator(new String[]{INT_ITERATION_FIELD}, new String[]{});
//            try {
//                ftgsIterator.nextField();
//                expectTerms(Arrays.asList(new IntTerm(Integer.MIN_VALUE,
//                                                      Arrays.asList(Pair.of(2, new long[]{1, 3}))),
//                                          new IntTerm(-1,
//                                                      Arrays.asList(Pair.of(1, new long[]{11, 3}))),
//                                          new IntTerm(0,
//                                                      Arrays.asList(Pair.of(1, new long[]{0, 1}),
//                                                                    Pair.of(2, new long[]{0, 2}))),
//                                          new IntTerm(1,
//                                                      Arrays.asList(Pair.of(1, new long[]{11, 3}))),
//                                          new IntTerm(Integer.MAX_VALUE,
//                                                      Arrays.asList(Pair.of(2, new long[]{1, 3})))),
//                            ftgsIterator);
//            } finally {
//                ftgsIterator.close();
//                session.close();
//            }
//        }
//    }
//
//    private SimpleFlamdexReader makeTestFlamdexReader() throws IOException {
//        final String dir = Files.getTempDirectory("flamdex-test", "foo");
//
//        SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, 20000L, true);
//
//        IntFieldWriter ifw = w.getIntFieldWriter(INT_ITERATION_FIELD);
//        addDataToIntTerm(ifw, Integer.MIN_VALUE, 5, 7, 8);
//        addDataToIntTerm(ifw, -1, 1, 2, 3);
//        addDataToIntTerm(ifw, 0, 4, 8, 9);
//        addDataToIntTerm(ifw, 1, 0, 1, 2);
//        addDataToIntTerm(ifw, Integer.MAX_VALUE, 5, 7, 8);
//        ifw.close();
//
//        ifw = w.getIntFieldWriter(DOCID_FIELD);
//        addDataToIntTerm(ifw, 0, 0);
//        addDataToIntTerm(ifw, 1, 1);
//        addDataToIntTerm(ifw, 2, 2);
//        addDataToIntTerm(ifw, 3, 3);
//        addDataToIntTerm(ifw, 4, 4);
//        addDataToIntTerm(ifw, 5, 5);
//        addDataToIntTerm(ifw, 6, 6);
//        addDataToIntTerm(ifw, 7, 7);
//        addDataToIntTerm(ifw, 8, 8);
//        addDataToIntTerm(ifw, 9, 9);
//        ifw.close();
//
//        ifw = w.getIntFieldWriter(METRIC_FIELD);
//        addDataToIntTerm(ifw, 0, 4, 7, 8, 9);
//        addDataToIntTerm(ifw, 1, 2, 5, 6);
//        addDataToIntTerm(ifw, 5, 0, 1, 3);
//        ifw.close();
//
//        StringFieldWriter sfw = w.getStringFieldWriter(STRING_ITERATION_FIELD);
//        addDataToStringTerm(sfw, "", 1, 4, 9);
//        addDataToStringTerm(sfw, "english", 1, 2, 3);
//        addDataToStringTerm(sfw, "日本語", 4, 5, 6);
//        sfw.close();
//
//        w.close();
//
//        return SimpleFlamdexReader.open(dir);
//    }
//
//    private static void addDataToStringTerm(StringFieldWriter sfw, String term, final int... docs) throws IOException {
//        sfw.nextTerm(term);
//        for (int doc : docs) {
//            sfw.nextDoc(doc);
//        }
//    }
//
//    private static void addDataToIntTerm(IntFieldWriter ifw, long term, final int... docs) throws IOException {
//        ifw.nextTerm(term);
//        for (int doc : docs) {
//            ifw.nextDoc(doc);
//        }
//    }
//
//    private static class IntTerm {
//        int term;
//        List<Pair<Integer, long[]>> groupStats;
//
//        private IntTerm(int term, List<Pair<Integer, long[]>> groupStats) {
//            this.term = term;
//            this.groupStats = groupStats;
//        }
//    }
//
//    private void expectTerms(List<IntTerm> terms, FTGSIterator ftgsIterator) {
//        long[] stats = new long[terms.get(0).groupStats.get(0).getSecond().length];
//        for (IntTerm term : terms) {
//            assertEquals(true, ftgsIterator.nextTerm());
//            assertEquals(term.term, ftgsIterator.termIntVal());
//            for (Pair<Integer, long[]> group : term.groupStats) {
//                assertEquals(true, ftgsIterator.nextGroup());
//                assertEquals((int)group.getFirst(), ftgsIterator.group());
//                ftgsIterator.groupStats(stats);
//                assertArrayEquals(group.getSecond(), stats);
//            }
//        }
//
//    }
//
//    private void testExpectedIntField(FTGSIterator ftgsIterator) {
//        assertEquals(true, ftgsIterator.nextField());
//        assertEquals(INT_ITERATION_FIELD, ftgsIterator.fieldName());
//        assertEquals(true, ftgsIterator.fieldIsIntType());
//        expectTerms(Arrays.asList(
//                new IntTerm(Integer.MIN_VALUE, Arrays.asList(Pair.of(2, new long[]{1}))),
//                new IntTerm(-1, Arrays.asList(Pair.of(1, new long[]{11}))),
//                new IntTerm(0, Arrays.asList(Pair.of(1, new long[]{0}), Pair.of(2, new long[]{0}))),
//                new IntTerm(1, Arrays.asList(Pair.of(1, new long[]{11}))),
//                new IntTerm(Integer.MAX_VALUE, Arrays.asList(Pair.of(2, new long[]{1})))
//        ), ftgsIterator);
//        assertEquals(false, ftgsIterator.nextGroup());
//        assertEquals(false, ftgsIterator.nextTerm());
//    }
//
//    private void testExpectedStringField(FTGSIterator ftgsIterator) {
//        long[] stats = new long[1];
//
//        assertEquals(true, ftgsIterator.nextField());
//        assertEquals(STRING_ITERATION_FIELD, ftgsIterator.fieldName());
//        assertEquals(false, ftgsIterator.fieldIsIntType());
//
//        assertEquals(true, ftgsIterator.nextTerm());
//        assertEquals("", ftgsIterator.termStringVal());
//        assertEquals(true, ftgsIterator.nextGroup());
//        assertEquals(1, ftgsIterator.group());
//        ftgsIterator.groupStats(stats);
//        assertArrayEquals(new long[]{5}, stats);
//        assertEquals(true, ftgsIterator.nextGroup());
//        assertEquals(2, ftgsIterator.group());
//        ftgsIterator.groupStats(stats);
//        assertArrayEquals(new long[]{0}, stats);
//        assertEquals(false, ftgsIterator.nextGroup());
//
//        assertEquals(true, ftgsIterator.nextTerm());
//        assertEquals("english", ftgsIterator.termStringVal());
//        assertEquals(true, ftgsIterator.nextGroup());
//        assertEquals(1, ftgsIterator.group());
//        ftgsIterator.groupStats(stats);
//        assertArrayEquals(new long[]{11}, stats);
//        assertEquals(false, ftgsIterator.nextGroup());
//
//        assertEquals(true, ftgsIterator.nextTerm());
//        assertEquals("日本語", ftgsIterator.termStringVal());
//        assertEquals(true, ftgsIterator.nextGroup());
//        assertEquals(1, ftgsIterator.group());
//        ftgsIterator.groupStats(stats);
//        assertArrayEquals(new long[]{0}, stats);
//        assertEquals(true, ftgsIterator.nextGroup());
//        assertEquals(2, ftgsIterator.group());
//        ftgsIterator.groupStats(stats);
//        assertArrayEquals(new long[]{2}, stats);
//        assertEquals(false, ftgsIterator.nextGroup());
//        assertEquals(false, ftgsIterator.nextTerm());
//    }
}
