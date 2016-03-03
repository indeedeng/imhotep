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

import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math.stat.descriptive.rank.Percentile;

import static org.junit.Assert.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.*;
import org.junit.rules.*;

import com.indeed.flamdex.simple.*;
import com.indeed.flamdex.writer.*;
import com.indeed.imhotep.*;
import com.indeed.imhotep.api.*;
import com.indeed.util.io.Files;

public class TestFTGSPerf {

    public static class ClusterSimulator {
        public static final int PORT = 8138;
        private ServerSocket serverSocket;
        private int nSplits;
        private int nStats;

        int port = PORT;

        ClusterSimulator(int nSplits, int nStats, int portmod) throws IOException {
            this.port += portmod;
            this.nSplits = nSplits;
            this.nStats = nStats;
            this.serverSocket = new ServerSocket(PORT);
        }

        public void readData() throws IOException {
            for (int i = 0; i < nSplits; i++) {
                Socket sock = new Socket("localhost", PORT);
                final InputStream is = sock.getInputStream();
                new Thread(new Runnable() {
                    
                    @Override
                    public void run() {
                        final byte[] buffer = new byte[8192];
                        try {
                            while (true) {
                                is.read(buffer);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }
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
        Runnable newRunnable(Socket socket, int i);
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
                        session.writeFTGSIteratorSplit(intFields, stringFields, i, numSplits, 0, socket);
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
        public List<String> terms;
    }

    private final int MAX_STRING_TERM_LEN = 32;
    private final long seed = 0xdeadbeef;
    private Random rand;

    private int numDocs;
    private int nMetrics;
    private String[] metricNames;
    private String[] stringFields;
    private String[] intFields;
    private List<String> shardNames = new ArrayList<>();
    private List<String> shardCopies = new ArrayList<>();

    private long[] validStats;
    private long[] testStats;

    private class UnusedDocsIds {
        final int[] docIds;
        int len;
        int nValidDocs;

        private UnusedDocsIds(int size) {
            this.docIds = new int[size];
            for (int i = 0; i < size; i++) {
                docIds[i] = i;
            }
            this.len = size;
            this.nValidDocs = size;
        }

        int size() {
            return this.nValidDocs;
        }

        int getRandId() {
            if (nValidDocs < len / 2) {
                this.compress();
            }

            while (true) {
                final int i = rand.nextInt(len);
                if (docIds[i] < 0) {
                    continue;
                }
                final int docId = docIds[i];
                docIds[i] = -1;
                nValidDocs --;
                return docId;
            }
        }

        private void compress() {
            int max = 0;
            int trailing = 0;

            for (int i = 0; i < this.len; i++) {
                if (docIds[i] < 0) {
                    continue;
                }
                docIds[trailing] = docIds[i];
                trailing++;
                if (max < docIds[i]) {
                    max = docIds[i];
                }
            }

            this.len = this.nValidDocs;
        }
    }

    private int[] selectDocIds(final UnusedDocsIds unusedDocsIds,
                               int nDocsToSelect) {
        final int[] selectedDocs = new int[nDocsToSelect];

        for (int n = 0; n < nDocsToSelect; n++) {
            selectedDocs[n] = unusedDocsIds.getRandId();
        }

        Arrays.sort(selectedDocs);
        return selectedDocs;
    }

    private void createFlamdexIntField(final String intFieldName,
                                       final MetricMaxSizes termGenerator,
                                       final int size,
                                       SimpleFlamdexWriter w) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        final int maxUnassignedDocs = (size < 100) ? 10 : 100;
        IntFieldWriter ifw = w.getIntFieldWriter(intFieldName);

        final UnusedDocsIds unusedDocIds = new UnusedDocsIds(size);

        TreeMap<Long, int[]> map = new TreeMap<>();
        while (unusedDocIds.size() > maxUnassignedDocs) {
            final long term = termGenerator.randVal();
            if (term == 0) {
                continue;
            }
            final int maxDocsPerTerm = Math.min(unusedDocIds.size(), maxTermDocs);
            final int numDocs = unusedDocIds.size() > 1 ? rand.nextInt(maxDocsPerTerm - 1) + 1 : 1;
            final int[] docIds = selectDocIds(unusedDocIds, numDocs);
            map.put(term, docIds);
        }

        for (Long term : map.keySet()) {
            ifw.nextTerm(term);
            for (int doc : map.get(term)) {
                ifw.nextDoc(doc);
            }
        }
        ifw.close();
    }

    private void createFlamdexStringField(final String stringFieldName,
                                          final int size,
                                          SimpleFlamdexWriter w,
                                          List<String> termsList) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        final int maxUnassignedDocs = (size < 100) ? 10 : 100;
        StringFieldWriter sfw = w.getStringFieldWriter(stringFieldName);

        final UnusedDocsIds unusedDocIds = new UnusedDocsIds(size);

        int i = 0;
        TreeMap<String, int[]> map = new TreeMap<>();
        while (unusedDocIds.size() > maxUnassignedDocs) {
            final String term = termsList.get(i);

            final int maxDocsPerTerm = Math.min(unusedDocIds.size(), maxTermDocs);
            final int numDocs = unusedDocIds.size() > 1 ? rand.nextInt(maxDocsPerTerm - 1) + 1 : 1;
            final int[] docIds = selectDocIds(unusedDocIds, numDocs);
            map.put(term, docIds);
        }

        for (String term : map.keySet()) {
            sfw.nextTerm(term);
            for (int doc : map.get(term)) {
                sfw.nextDoc(doc);
            }
        }
        sfw.close();
    }

    private void generateShard(final String dir, List<FieldDesc> fieldDescs) throws IOException {
        // find max # of docs
        long nDocs = 0;
        for (FieldDesc fd : fieldDescs) {
            nDocs = (fd.numDocs > nDocs) ? fd.numDocs : nDocs;
        }

        final SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, nDocs, true);

        for (FieldDesc fd : fieldDescs) {
            if (fd.isIntfield) {
                createFlamdexIntField(fd.name, fd.termGenerator, fd.numDocs, w);
            } else {
                createFlamdexStringField(fd.name, fd.numDocs, w, fd.terms);
            }
        }

        w.close();
    }

    public enum MetricMaxSizes {
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

    private MTImhotepLocalMultiSession createMultisession(List<String> shardDirs,
                                                          String[] metricNames,
                                                          boolean useNativeFTGS)
        throws ImhotepOutOfMemoryException, IOException {
        ImhotepLocalSession[] localSessions = new ImhotepLocalSession[shardDirs.size()];
        for (int i = 0; i < shardDirs.size(); i++) {
            final String dir = shardDirs.get(i);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            final ImhotepLocalSession localSession = new ImhotepJavaLocalSession(r);
            localSessions[i] = localSession;
        }

        AtomicLong foo = new AtomicLong(1000000000);
        final MTImhotepLocalMultiSession mtSession;
        final MemoryReservationContext mrc =
            new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE));
        mtSession = new MTImhotepLocalMultiSession(localSessions, mrc, foo, useNativeFTGS);

        mtSession.randomMultiRegroup(metricNames[0],
                                     true,
                                     "12345",
                                     1,
                                     new double[] { 0.1, 0.4, 0.7 },
                                     new int[] { 0, 2, 3, 4 });

        long time = System.nanoTime();
        for (String metric : metricNames) {
            mtSession.pushStat(metric);
        }
        System.out.println("push stats: " + timeStamp(System.nanoTime() - time));

        return mtSession;
    }

    private String randomString(int len) {
        return RandomStringUtils.random(len, 0, 0, true, false, null, rand);
    }


    private void setUpShards(final File shardDir) throws IOException, InterruptedException {
        System.err.print("building shards...");
        shardDir.mkdir();

        String fieldName = randomString(MAX_STRING_TERM_LEN-1);
        metricNames = new String[nMetrics];
        for (int i = 0; i < nMetrics; i++) {
            metricNames[i] = randomString(MAX_STRING_TERM_LEN-1);
        }

        Arrays.sort(metricNames);

        stringFields = new String[]{fieldName};
        intFields = new String[0];

        final List<FieldDesc> fieldDescs = createShardProfile(numDocs, nMetrics, fieldName, metricNames);
        for (int i = 0; i < 5; i++) {
            final File shard     = new File(shardDir, "native-ftgs-test-" + i);
            final File shardCopy = new File(shardDir, "native-ftgs-verify-" + i);
            generateShard(shard.getAbsolutePath(), fieldDescs);
            shardNames.add(shard.getAbsolutePath());
            FileUtils.copyDirectory(shard, shardCopy);
            shardCopies.add(shardCopy.getAbsolutePath());
        }
        System.err.println("complete");
    }

    private void recycleShards(final File shardDir) throws IOException, InterruptedException {
        System.err.print("recycling shards...");
        metricNames  = new String[0];
        stringFields = new String[0];
        intFields    = new String[0];

        for (int i = 0; i < 5; i++) {
            final File shard     = new File(shardDir, "native-ftgs-test-" + i);
            final File shardCopy = new File(shardDir, "native-ftgs-verify-" + i);
            if (i == 0) {
                final SimpleFlamdexReader reader = SimpleFlamdexReader.open(shard.getAbsolutePath());
                metricNames  = reader.getAvailableMetrics().toArray(metricNames);
                stringFields = reader.getStringFields().toArray(stringFields);
                //                intFields    = reader.getIntFields().toArray(intFields);
            }
            shardNames.add(shard.getAbsolutePath());
            shardCopies.add(shardCopy.getAbsolutePath());
        }
        System.err.println("complete");
    }

    @Before
    public void setUp() throws IOException, InterruptedException {
        rand     = new Random(seed);
        numDocs  = 10000000;
        nMetrics = 16;
        System.out.println("Random seed: " + seed);
        System.out.println("nMetrics: " + nMetrics);

        File shardDir = new File("/tmp/shard");
        if (!shardDir.exists())
            setUpShards(shardDir);
        else
            recycleShards(shardDir);

        validStats = new long[nMetrics];
        testStats = new long[nMetrics];

        System.gc();
    }

    @After
    public void tearDown()
    { }

    private final int ITERATIONS = 1000;

    private List<FieldDesc> createShardProfile(int numDocs,
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
            final int numTerms = 4 * (1 << 20);  // 4M
            fd.terms = new ArrayList<>(numTerms);
            for (int i = 0; i < numTerms; i++) {
                final String term = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
                fd.terms.add(term);
            }
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

        return fieldDescs;
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
                    assertEquals(v, t);
                } else {
                    String v = valid.termStringVal();
                    final String t = test.termStringVal();
                    while (!v.equals(t)) {
                        /* check to see if there are any groups in the term */
                        if (valid.nextGroup()) {
                            break;
                        }
                        valid.nextTerm();
                        v = valid.termStringVal();
                    }
                    assertEquals(v, t);
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

    private void readAllIterator(FTGSIterator iterator, int numStats) {
        while (iterator.nextField()) {
            while (iterator.nextTerm()) {
                if (iterator.fieldIsIntType()) {
                    final long v = iterator.termIntVal();
                } else {
                    String v = iterator.termStringVal();
                }
                while (iterator.nextGroup()) {
                    iterator.groupStats(validStats);
                }
            }
        }
    }

    private final String timeStamp(long nanos) {
        final double seconds = nanos / 1000000000.0;
        return String.format("%.6f", seconds);
    }

    /*

    @Test
    public void testJava() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
        System.err.println("starting...");
        final double[] times   = new double[ITERATIONS];

        final MTImhotepLocalMultiSession session;
        session = createMultisession(shardCopies, metricNames, false);
        for (int i = 0; i < ITERATIONS; i++) {
            long time = System.nanoTime();
            FTGSIterator iterator = session.getFTGSIterator(intFields, stringFields);
            readAllIterator(iterator, nMetrics);
            times[i] = (System.nanoTime() - time) / 1000000000.0;
            iterator.close();
        }

        Percentile percentile = new Percentile();
        StandardDeviation sd = new StandardDeviation();
        double[] items = new double[] { 10.0, 50.0, 75.0, 90.0, 99.0, 99.99 };
        System.out.printf("\t");
        for (int i = 0; i < items.length; ++i) {
            System.out.printf("%.2f\t\t", items[i]);
        }
        System.out.printf("σ\n");

        System.out.printf("java\t");
        for (int i = 0; i < items.length; ++i) {
            System.out.printf("%.6f\t", percentile.evaluate(times, items[i]));
        }
        System.out.printf("%.6f\n", sd.evaluate(times));
    }

    */
//    @Test
    public void testNative() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
        System.err.println("starting...");
        for (int z = 0; z < 100; ++z) {
            final double[] times = new double[ITERATIONS];

            final MTImhotepLocalMultiSession session;
            session = createMultisession(shardNames, metricNames, true);
            long begin = System.nanoTime();
            session.updateMulticaches();
            System.out.println("update multicaches: " + timeStamp(System.nanoTime() - begin));

            RunnableFactory factory = new WriteFTGSRunner(session, intFields, stringFields, 8);

            /*
              for (int i = 0; i < ITERATIONS; i++) {
              ClusterSimulator simulator = new ClusterSimulator(8, nMetrics, i);
              Thread t = simulator.simulateServer(factory);
              long time = System.nanoTime();
              RawFTGSIterator iterator = simulator.getIterator();
              readAllIterator(iterator, nMetrics);
              times[i] = (System.nanoTime() - time) / 1000000000.0;

              t.join();
              simulator.close();
              }
            */
            session.close();
        }
        /*
          Percentile percentile = new Percentile();
          StandardDeviation sd = new StandardDeviation();
          double[] items = new double[] { 10.0, 50.0, 75.0, 90.0, 99.0, 99.99  };
          System.out.printf("\t");
          for (int i = 0; i < items.length; ++i) {
          System.out.printf("%.2f\t\t", items[i]);
          }
          System.out.printf("σ\n");

          System.out.printf("native\t");
          for (int i = 0; i < items.length; ++i) {
          System.out.printf("%.6f\t", percentile.evaluate(times, items[i]));
          }
          System.out.printf("%.6f\n", sd.evaluate(times));
        */
    }
}
