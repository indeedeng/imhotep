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

import fj.P;
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
 * @author darren
 */
public class TestNativeFlamdexFTGSIterator {
    // this field is silly and exists for regrouping purposes.
    private static final String DOCID_FIELD = "docIdField";

    enum BitsetOptimizationLevel {
        DONT_OPTIMIZE,
        OPTIMIZE,
    }

    public static class ClusterSimulator {
        public static final int PORT = 8138;
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

        public Thread simulateServer(final RunnableFactory factory) throws IOException {
            final Thread result = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread[] threads = new Thread[nSplits];
                        for (int i = 0; i < nSplits; i++) {
                            Socket sock = serverSocket.accept();
                            Thread t = new Thread(factory.newRunnable(sock, i));
                            t.start();
                            threads[i] = t;
                        }
                        
                        for (Thread t : threads) {
                            t.join();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            result.start();
            return result;
        }
        
        public void close() throws IOException {
            this.serverSocket.close();
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
                    try {
                        session.writeFTGSIteratorSplit(intFields, stringFields, i, numSplits, socket);
                        socket.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (ImhotepOutOfMemoryException e) {
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
        public MetricMaxSizes termGenerator;
    }

    private final Random rand = new Random();
    private final int MAX_STRING_TERM_LEN = 128;

    private void createFlamdexIntField(final String dir,
                                       final String intFieldName,
                                       final MetricMaxSizes termGenerator,
                                       final int size,
                                       SimpleFlamdexWriter w) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        IntFieldWriter ifw = w.getIntFieldWriter(intFieldName);
        List<Integer> docs = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            docs.add(i);
        }

        Map<Long, List<Integer>> map = Maps.newTreeMap();
        while (!docs.isEmpty()) {
            final List<Integer> selectedDocs;
            final long term = termGenerator.randVal();
            if (term == 0) {
                continue;
            }
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
    }

    private void createFlamdexStringField(final String dir,
                                       final String stringFieldName,
                                       final int size,
                                       SimpleFlamdexWriter w) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        StringFieldWriter sfw = w.getStringFieldWriter(stringFieldName);
        List<Integer> docs = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            docs.add(i);
        }

        Map<String, List<Integer>> map = Maps.newTreeMap();
        while (!docs.isEmpty()) {
            String term = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
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
    }

    private String generateShard(List<FieldDesc> fieldDescs, int totalNDocs) throws IOException {
        final String dir = Files.getTempDirectory("native-ftgs-test", "test");
        
        // find max # of docs
        long nDocs = 0;
        for (FieldDesc fd : fieldDescs) {
            nDocs = (fd.numDocs > nDocs) ? fd.numDocs : nDocs;
        }
        
        final SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, nDocs, true);

        for (FieldDesc fd : fieldDescs) {
//            System.out.println(fd.name);
            if (fd.isIntfield) {
                createFlamdexIntField(dir, fd.name, fd.termGenerator, fd.numDocs, w);
            } else {
                createFlamdexStringField(dir, fd.name, fd.numDocs, w);
            }
        }

        // create DOC_ID field
        IntFieldWriter ifw = w.getIntFieldWriter(DOCID_FIELD);
        for (int i = 0; i < totalNDocs; i++) {
            ifw.nextTerm(i);
            ifw.nextDoc(i);
        }
        ifw.close();

        w.close();
        return dir;
    }

    private String copyShard(String dir) throws IOException {
        final String new_dir = Files.getTempDirectory("native-ftgs-test", "verify-copy");

        FileUtils.copyDirectory(new File(dir), new File(new_dir));

        return new_dir;
    }

    public enum MetricMaxSizes {
        ALL("doc_ids") {
            private int counter = 0;

            @Override
            public long randVal() {
                final int ret = this.counter;
                this.counter ++;
                return ret;
            }
        },
        LONG(Long.MAX_VALUE, Long.MIN_VALUE, "long") {
            @Override
            public long randVal() {
                return this.foo.nextLong();
            }
        },
        INT(Integer.MAX_VALUE, Integer.MIN_VALUE, "int"){
            @Override
            public long randVal() {
                return this.foo.nextInt();
            }
        },
        CHAR(65535, 0, "unsigned short"){
            @Override
            public long randVal() {
                return (char)this.foo.nextInt();
            }
        },
        SHORT(Short.MAX_VALUE, Short.MIN_VALUE, "short") {
            @Override
            public long randVal() {
                return (short)this.foo.nextInt();
            }
        },
        SIGNED_BYTE(Byte.MAX_VALUE, Byte.MIN_VALUE, "signed byte") {
            @Override
            public long randVal() {
                byte[] b = new byte[1];
                
                this.foo.nextBytes(b);
                return b[0];
            }
        },
        BYTE(255,0,"unsigned byte") {
            @Override
            public long randVal() {
                byte[] b = new byte[1];
                
                this.foo.nextBytes(b);
                return b[0] - Byte.MIN_VALUE;
            }
        },
        BINARY(1, 0, "binary") {
            @Override
            public long randVal() {
                if (this.foo.nextBoolean())
                    return 1;
                else
                    return 0;
            }
        };

        protected final long maxVal;
        protected final long minVal;
        protected final String name;
        protected final Random foo = new Random();

        MetricMaxSizes(long maxVal, long minVal, String name) {
            this.maxVal = maxVal;
            this.minVal = minVal;
            this.name = name;
        }

        MetricMaxSizes(String name) {
            this(Integer.MAX_VALUE, 0, name);
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

        public abstract long randVal();

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


    private MTImhotepLocalMultiSession createMultisession(List<String> shardDirs,
                                                          String[] metricNames,
                                                          BitsetOptimizationLevel level,
                                                          int[] boundaries) throws ImhotepOutOfMemoryException, IOException {
        ImhotepLocalSession[] localSessions = new ImhotepLocalSession[shardDirs.size()];
        for (int i = 0; i < shardDirs.size(); i++) {
            final String dir = shardDirs.get(i);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            final ImhotepLocalSession localSession;
            localSession = new ImhotepLocalSession(r, level == BitsetOptimizationLevel.OPTIMIZE);
            localSessions[i] = localSession;
        }

        final ExecutorService executor = Executors.newCachedThreadPool();
        AtomicLong foo = new AtomicLong(100000000);
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
                                                                                       boundaries[0],
                                                                                       null,
                                                                                       true),
                                                                  2,
                                                                  1)});
        System.out.println(Arrays.toString(metricNames));
        for (String metric : metricNames) {
            mtSession.pushStat(metric);
        }

        return mtSession;
    }

    private String randomString(int len) {
        return RandomStringUtils.random(len, 0, 0, true, false, null, rand);
    }

    @Test
    public void testSingleShard() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
//        final long seed = rand.nextLong();
        final long seed = 6856357630770252391L;
        rand.setSeed(seed);
        System.out.println("Random seed: " + seed);
        final int numDocs = rand.nextInt(1 << 16) + 1;  // 64K

        // need at least one
//        final int nMetrics = rand.nextInt(MAX_N_METRICS - 1) + 1;
//        String fieldName = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
//        String[] metricNames = new String[nMetrics];
//        for (int i = 0; i < nMetrics; i++) {
//            metricNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
//        }
        final int nMetrics0 = rand.nextInt(MAX_N_METRICS - 1) + 1;
        String fieldName0 = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
        String[] metricNames0 = new String[nMetrics0];
        for (int i = 0; i < nMetrics0; i++) {
            metricNames0[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
        }

//        String shardname = createNewShard(numDocs, nMetrics, fieldName, metricNames);
//        System.out.println(shardname);
//        String shardCopy = copyShard(shardname);
//        System.out.println(shardCopy);
        final String shardname = "/tmp/native-ftgs-test3621273677520696910test";
        final String shardCopy = "/tmp/native-ftgs-test1569059667865154452verify-copy";

        final String fieldName = "jJBogivDhWOCJAowTjlBYnqNlHUyrjoxcsEqEgpIzeOASFBtSStmIdecdbJRtCwufbVbiCRrdkMDnRvtYBJfvSNhyiIFPkWHzJDkMXtaPFkbcYBrbHpDbZzVV";
        final int nMetrics = metricNames0.length;
        final String[] metricNames = metricNames0;

        final String[] stringFields = new String[]{fieldName};
        final String[] intFields = new String[0];

        final int nGroups = 5;
        final int[] boundaries = new int[nGroups];

        int prevBoundary = 0;
        for (int i = 0; i < nGroups; i++) {
            final int newBoundary = rand.nextInt(numDocs - prevBoundary);
            boundaries[i] = newBoundary;
            prevBoundary = newBoundary;
        }

        System.out.println("group 0 boundary: " + boundaries[0]);

        final MTImhotepLocalMultiSession verificationSession;
        verificationSession = createMultisession(Arrays.asList(shardCopy),
                                                 metricNames,
                                                 BitsetOptimizationLevel.DONT_OPTIMIZE,
                                                 boundaries);
        FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields);

        final MTImhotepLocalMultiSession testSession;
        testSession = createMultisession(Arrays.asList(shardname),
                                           metricNames,
                                           BitsetOptimizationLevel.DONT_OPTIMIZE,
                                           boundaries);
        RunnableFactory factory = new WriteFTGSRunner(testSession, intFields, stringFields, 8);
        ClusterSimulator simulator = new ClusterSimulator(8, nMetrics);

        Thread t = simulator.simulateServer(factory);
        RawFTGSIterator ftgsIterator = simulator.getIterator();

        compareIterators(ftgsIterator, verificationIter, nMetrics);

        t.join();
        
        simulator.close();
        verificationIter.close();
    }

//    @Test
//    public void test10Shards() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
//        final long seed = rand.nextLong();
////        final long seed = -4122356988999045667L;
//        rand.setSeed(seed);
//        System.out.println("Random seed: " + seed);
//        final int numDocs = rand.nextInt(1 << 16) + 1;  // 64K
//
//        // need at least one
//        final int nMetrics = rand.nextInt(MAX_N_METRICS - 1) + 1;
//        String fieldName = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
//        String[] metricNames = new String[nMetrics];
//        for (int i = 0; i < nMetrics; i++) {
//            metricNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
//        }
//
//
//        List<String> shardNames = new ArrayList<>();
//        List<String> shardCopies = new ArrayList<>();
//        for (int i = 0; i < 10; i++) {
//            final String shard = createNewShard(numDocs, nMetrics, fieldName, metricNames);
//            shardNames.add(shard);
//            shardCopies.add(copyShard(shard));
//        }
//
//        final String[] stringFields = new String[]{fieldName};
//        final String[] intFields = new String[0];
//
//        FTGSIterator verificationIter = getVerificationIterator(shardCopies,
//                                                                metricNames,
//                                                                BitsetOptimizationLevel.DONT_OPTIMIZE,
//                                                                stringFields,
//                                                                intFields,
//                                                                numDocs);
//
//        final MTImhotepLocalMultiSession session;
//        session = makeTestSession(shardNames,
//                                  metricNames,
//                                  BitsetOptimizationLevel.DONT_OPTIMIZE);
//
//        RunnableFactory factory = new WriteFTGSRunner(session, intFields, stringFields, 8);
//        ClusterSimulator simulator = new ClusterSimulator(8, nMetrics);
//
//        Thread t = simulator.simulateServer(factory);
//        RawFTGSIterator ftgsIterator = simulator.getIterator();
//
//        compareIterators(ftgsIterator, verificationIter, nMetrics);
//
//        t.join();
//
//        simulator.close();
//        verificationIter.close();
//    }
//
    private String createNewShard(int numDocs,
                                  int nMetrics,
                                  String fieldName,
                                  String[] metricNames) throws IOException {
        final List<FieldDesc> fieldDescs = new ArrayList<>(nMetrics);

        // create field
        {
            final FieldDesc fd = new FieldDesc();
            fd.isIntfield = false;
            fd.name = fieldName;
            fd.numDocs = numDocs;
            fieldDescs.add(fd);
        }

        // create metrics
        for (int i = 0; i < nMetrics; i++) {
            final FieldDesc fd = new FieldDesc();
            fd.isIntfield = true;
            fd.name = metricNames[i];
            fd.termGenerator = MetricMaxSizes.getRandomSize();
            fd.numDocs = numDocs;
            fieldDescs.add(fd);
        }

        return generateShard(fieldDescs, numDocs);
    }

    private void compareIterators(FTGSIterator test, FTGSIterator valid, int numStats) {
        final long[] validStats = new long[numStats];
        final long[] testStats = new long[numStats];

        while (valid.nextField()) {
            assertTrue(test.nextField());
            assertEquals(valid.fieldIsIntType(), test.fieldIsIntType());
            assertEquals(valid.fieldName(), test.fieldName());
            while (valid.nextTerm()) {
                assertTrue(test.nextTerm());
                assertEquals(valid.fieldIsIntType(), test.fieldIsIntType());
                if (valid.fieldIsIntType()) {
                    final long v = valid.termIntVal();
                    final long t = test.termIntVal();
                    System.out.println("valid: " + v + ", test: " + t);
                    assertEquals(v, t);
                } else {
                    final String v = valid.termStringVal();
                    final String t = test.termStringVal();
                    System.out.println("valid: " + v + ", test: " + t);
                    assertEquals(v, t);
                }
                assertEquals(valid.termDocFreq(), test.termDocFreq());
                while (valid.nextGroup()) {
                    System.out.println(Integer.toString(valid.group()));
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
