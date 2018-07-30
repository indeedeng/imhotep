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

//import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
//import org.apache.commons.math.stat.descriptive.rank.Percentile;

import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFTGSPerf {

    public static class ClusterSimulator {
        public static final int PORT = 8138;
        private final ServerSocket serverSocket;
        private final int nSplits;
        private final int nStats;

        int port = PORT;

        ClusterSimulator(final int nSplits, final int nStats, final int portmod) throws IOException {
            this.port += portmod;
            this.nSplits = nSplits;
            this.nStats = nStats;
            this.serverSocket = new ServerSocket(PORT);
        }

        public void readData() throws IOException {
            for (int i = 0; i < nSplits; i++) {
                final Socket sock = new Socket("localhost", PORT);
                final InputStream is = sock.getInputStream();
                new Thread(new Runnable() {
                    
                    @Override
                    public void run() {
                        final byte[] buffer = new byte[8192];
                        try {
                            while (true) {
                                is.read(buffer);
                            }
                        } catch (final IOException e) {
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
                        final Thread[] threads = new Thread[nSplits];
                        for (int i = 0; i < nSplits; i++) {
                            final Socket sock = serverSocket.accept();
                            final Thread t = new Thread(factory.newRunnable(sock, i));
                            t.start();
                            threads[i] = t;
                        }

                        for (final Thread t : threads) {
                            t.join();
                        }
                    } catch (final IOException | InterruptedException e) {
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
    private final List<Path> shardNames = new ArrayList<>();
    private final List<String> shardCopies = new ArrayList<>();

    private long[] validStats;

    private class UnusedDocsIds {
        final int[] docIds;
        int len;
        int nValidDocs;

        private UnusedDocsIds(final int size) {
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
                               final int nDocsToSelect) {
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
                                       final SimpleFlamdexWriter w) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        final int maxUnassignedDocs = (size < 100) ? 10 : 100;
        try (final IntFieldWriter ifw = w.getIntFieldWriter(intFieldName)) {

            final UnusedDocsIds unusedDocIds = new UnusedDocsIds(size);

            final TreeMap<Long, int[]> map = new TreeMap<>();
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

            for (final Map.Entry<Long, int[]> terms : map.entrySet()) {
                ifw.nextTerm(terms.getKey());
                for (final int doc : terms.getValue()) {
                    ifw.nextDoc(doc);
                }
            }
        }
    }

    private void createFlamdexStringField(final String stringFieldName,
                                          final int size,
                                          final SimpleFlamdexWriter w,
                                          final List<String> termsList) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        final int maxUnassignedDocs = (size < 100) ? 10 : 100;
        try (final StringFieldWriter sfw = w.getStringFieldWriter(stringFieldName)) {

            final UnusedDocsIds unusedDocIds = new UnusedDocsIds(size);

            final int i = 0;
            final TreeMap<String, int[]> map = new TreeMap<>();
            while (unusedDocIds.size() > maxUnassignedDocs) {
                final String term = termsList.get(i);

                final int maxDocsPerTerm = Math.min(unusedDocIds.size(), maxTermDocs);
                final int numDocs = unusedDocIds.size() > 1 ? rand.nextInt(maxDocsPerTerm - 1) + 1 : 1;
                final int[] docIds = selectDocIds(unusedDocIds, numDocs);
                map.put(term, docIds);
            }

            for (final Map.Entry<String, int[]> termEntry : map.entrySet()) {
                sfw.nextTerm(termEntry.getKey());
                for (final int doc : termEntry.getValue()) {
                    sfw.nextDoc(doc);
                }
            }
        }
    }

    private void generateShard(final Path dir, final List<FieldDesc> fieldDescs) throws IOException {
        // find max # of docs
        long nDocs = 0;
        for (final FieldDesc fd : fieldDescs) {
            nDocs = (fd.numDocs > nDocs) ? fd.numDocs : nDocs;
        }

        try (final SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, nDocs, true)) {

            for (final FieldDesc fd : fieldDescs) {
                if (fd.isIntfield) {
                    createFlamdexIntField(fd.name, fd.termGenerator, fd.numDocs, w);
                } else {
                    createFlamdexStringField(fd.name, fd.numDocs, w, fd.terms);
                }
            }
        }
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
                final byte[] b = new byte[1];

                this.foo.nextBytes(b);
                return b[0];
            }
        },
        BYTE(255,0,"unsigned byte") {
            @Override
            public long randVal() {
                final byte[] b = new byte[1];

                this.foo.nextBytes(b);
                return b[0] - Byte.MIN_VALUE;
            }
        },
        BINARY(1, 0, "binary") {
            @Override
            public long randVal() {
                if (this.foo.nextBoolean()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };

        protected final long maxVal;
        protected final long minVal;
        protected final String name;
        protected final Random foo = new Random();

        MetricMaxSizes(final long maxVal, final long minVal, final String name) {
            this.maxVal = maxVal;
            this.minVal = minVal;
            this.name = name;
        }

        MetricMaxSizes(final String name) {
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

    private MTImhotepLocalMultiSession createMultisession(final List<Path> shardDirs,
                                                          final String[] metricNames)
        throws ImhotepOutOfMemoryException, IOException {
        final ImhotepLocalSession[] localSessions = new ImhotepLocalSession[shardDirs.size()];
        for (int i = 0; i < shardDirs.size(); i++) {
            final Path dir = shardDirs.get(i);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            final ImhotepLocalSession localSession = new ImhotepJavaLocalSession("TestFTGSPerf", r);
            localSessions[i] = localSession;
        }

        final AtomicLong foo = new AtomicLong(1000000000);
        final MTImhotepLocalMultiSession mtSession;
        final MemoryReservationContext mrc =
            new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE));
        mtSession = new MTImhotepLocalMultiSession("TestFTGSPerf", localSessions, mrc, foo, "", "");

        mtSession.randomMultiRegroup(metricNames[0],
                                     true,
                                     "12345",
                                     1,
                                     new double[] { 0.1, 0.4, 0.7 },
                                     new int[] { 0, 2, 3, 4 });

        final long time = System.nanoTime();
        for (final String metric : metricNames) {
            mtSession.pushStat(metric);
        }
        System.out.println("push stats: " + timeStamp(System.nanoTime() - time));

        return mtSession;
    }

    private String randomString(final int len) {
        return RandomStringUtils.random(len, 0, 0, true, false, null, rand);
    }


    private void setUpShards(final File shardDir) throws IOException, InterruptedException {
        System.err.print("building shards...");
        shardDir.mkdir();

        final String fieldName = randomString(MAX_STRING_TERM_LEN-1);
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
            generateShard(shard.toPath(), fieldDescs);
            shardNames.add(shard.toPath());
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
                final SimpleFlamdexReader reader = SimpleFlamdexReader.open(shard.toPath());
                metricNames  = reader.getAvailableMetrics().toArray(metricNames);
                stringFields = reader.getStringFields().toArray(stringFields);
                //                intFields    = reader.getIntFields().toArray(intFields);
            }
            shardNames.add(shard.toPath());
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

        final File shardDir = new File("/tmp/shard");
        if (!shardDir.exists()) {
            setUpShards(shardDir);
        } else {
            recycleShards(shardDir);
        }

        validStats = new long[nMetrics];
        final long[] testStats = new long[nMetrics];

        System.gc();
    }

    @After
    public void tearDown()
    { }

    private final int ITERATIONS = 1000;

    private List<FieldDesc> createShardProfile(final int numDocs,
                                               final int nMetrics,
                                               final String fieldName,
                                               final String[] metricNames) throws IOException {
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

    private void compareIterators(final FTGSIterator test, final FTGSIterator valid, final int numStats) {
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

    private void readAllIterator(final FTGSIterator iterator, final int numStats) {
        while (iterator.nextField()) {
            while (iterator.nextTerm()) {
                if (iterator.fieldIsIntType()) {
                    final long v = iterator.termIntVal();
                } else {
                    final String v = iterator.termStringVal();
                }
                while (iterator.nextGroup()) {
                    iterator.groupStats(validStats);
                }
            }
        }
    }

    private String timeStamp(final long nanos) {
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
}
