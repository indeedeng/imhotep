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

import static org.junit.Assert.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.indeed.flamdex.simple.*;
import com.indeed.flamdex.writer.*;
import com.indeed.imhotep.*;
import com.indeed.imhotep.api.*;
import com.indeed.util.io.Files;

/**
 * @author darren
 */
public class TestNativeFlamdexFTGSIterator {

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
        public List<String> terms;
    }

    private final Random rand = new Random();
    private final int MAX_STRING_TERM_LEN = 128;

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
                                       final int numDocs,
                                       SimpleFlamdexWriter w) throws IOException {
        final int numTerms       = 24;
        final int numDocsPerTerm = numDocs / numTerms;

        ArrayList<Long> terms = new ArrayList();
        // Long currentTerm = termGenerator.randVal();
        for (long count = 0; count < numTerms; ++count) {
            // terms.add(currentTerm);
            terms.add(count);
            // currentTerm++;
        }
        // Collections.sort(terms);

        int doc = 0;
        IntFieldWriter ifw = w.getIntFieldWriter(intFieldName);
        for (Long term: terms) {
            ifw.nextTerm(term);
            for (int count = 0; count < numDocsPerTerm; ++count) {
                ifw.nextDoc(doc);
                ++doc;
            }
        }
        ifw.close();
    }

    private void createFlamdexStringField(final String stringFieldName,
                                          final int numDocs,
                                          SimpleFlamdexWriter w,
                                          List<String> terms) throws IOException {
        StringFieldWriter sfw = w.getStringFieldWriter(stringFieldName);
        final int numDocsPerTerm = numDocs / terms.size();

        ArrayList<String> sortedTerms = new ArrayList(terms);
        Collections.sort(sortedTerms);

        int doc = 0;
        for (String term: sortedTerms) {
            sfw.nextTerm(term);
            for (int count = 0; count < numDocsPerTerm; ++count) {
                sfw.nextDoc(doc);
                ++doc;
            }
        }
        sfw.close();
    }

    private String generateShard(List<FieldDesc> fieldDescs) throws IOException {
        final String dir = Files.getTempDirectory("native-ftgs-test", "test");

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
        return dir;
    }

    private String copyShard(String dir) throws IOException {
        final String new_dir = Files.getTempDirectory("native-ftgs-test", "verify-copy");

        FileUtils.copyDirectory(new File(dir), new File(new_dir));

        return new_dir;
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
                                                          String[] metricNames)
        throws ImhotepOutOfMemoryException, IOException {
        ImhotepLocalSession[] localSessions = new ImhotepLocalSession[shardDirs.size()];
        for (int i = 0; i < shardDirs.size(); i++) {
            final String dir = shardDirs.get(i);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            final ImhotepLocalSession localSession;
            localSession = new ImhotepLocalSession(r, false);
            localSessions[i] = localSession;
        }

        final ExecutorService executor = Executors.newCachedThreadPool();
        AtomicLong foo = new AtomicLong(1000000000);
        final MTImhotepLocalMultiSession mtSession;
        mtSession = new MTImhotepLocalMultiSession(localSessions,
                                                   new MemoryReservationContext(new ImhotepMemoryPool(
                                                           Long.MAX_VALUE)),
                                                   executor,
                                                   foo,
                                                   true);

        /*
        mtSession.randomMultiRegroup(metricNames[0],
                                     true,
                                     "12345",
                                     1,
                                     new double[] { 0.1, 0.4, 0.7 },
                                     new int[] { 0, 2, 3, 4 });
        */

//        System.out.println(Arrays.toString(metricNames));
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
        final long seed = 42;
        rand.setSeed(seed);
        System.out.println("Random seed: " + seed);
        final int numDocs = 10000;

        final int numFields = 3;
        final int nMetrics = numFields;
        final int nIntFields = numFields;
        final int nStringFields = numFields;
        String[] stringFieldNames = new String[nStringFields];
        String[] intFieldNames = new String[nIntFields];
        String[] metricNames = new String[nMetrics];
        for (int i = 0; i < numFields; ++i) {
            stringFieldNames[i] = String.format("str-%d", i);
            intFieldNames[i] = String.format("int-%d", i);
            metricNames[i] = String.format("metric-%d", i);
        }

        final List<FieldDesc> fieldDescs =
            createShardProfile(numDocs, nMetrics,
                               nStringFields, stringFieldNames, nIntFields,
                               intFieldNames, metricNames, false);
        final String shardname = generateShard(fieldDescs);
        System.out.println(shardname);
        String shardCopy = copyShard(shardname);
        System.out.println(shardCopy);

        final String[] stringFields = stringFieldNames;
        final String[] intFields = intFieldNames;
        System.out.println("string fields: \n" + Arrays.toString(stringFields));
        System.out.println("int fields: \n" + Arrays.toString(intFields));

        final MTImhotepLocalMultiSession verificationSession;
        verificationSession = createMultisession(Arrays.asList(shardCopy), metricNames);
        FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields);

        final MTImhotepLocalMultiSession testSession;
        testSession = createMultisession(Arrays.asList(shardname),metricNames);
        RunnableFactory factory = new WriteFTGSRunner(testSession, intFields, stringFields, 8);
        ClusterSimulator simulator = new ClusterSimulator(8, nMetrics);

        Thread t = simulator.simulateServer(factory);
        RawFTGSIterator ftgsIterator = simulator.getIterator();

        // System.err.println("java: ");
        // dumpIterator(verificationIter, nMetrics);

        // System.err.println("native:");
        // dumpIterator(ftgsIterator, nMetrics);

        compareIterators(ftgsIterator, verificationIter, nMetrics);

        t.join();

        simulator.close();
        verificationIter.close();
    }

    @Test
    public void testSingleShardBinary() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
        //        final long seed = -5222463107059882979L;
        final long seed = 42;
        rand.setSeed(seed);
        System.out.println("Random seed: " + seed);
        final int numDocs = rand.nextInt(1 << 16) + 1;  // 64K

        // need at least one
        final int nMetrics = 3;
        final int nIntFields = rand.nextInt(3 - 1) + 1;
        final int nStringFields = rand.nextInt(3 - 1) + 1;
        String[] stringFieldNames = new String[nStringFields];
        for (int i = 0; i < nStringFields; i++) {
            stringFieldNames[i] = String.format("str-%d", i);
        }
        String[] intFieldNames = new String[nIntFields];
        for (int i = 0; i < nIntFields; i++) {
            intFieldNames[i] = String.format("int-%d", i);
        }
        String[] metricNames = new String[nMetrics];
        for (int i = 0; i < nMetrics; i++) {
            metricNames[i] = String.format("metric-%d", i);
        }

        final List<FieldDesc> fieldDescs = createShardProfile(numDocs,
                                                              nMetrics,
                                                              nStringFields,
                                                              stringFieldNames,
                                                              nIntFields,
                                                              intFieldNames,
                                                              metricNames,
                                                              true);
        final String shardname = generateShard(fieldDescs);
        System.out.println(shardname);
        String shardCopy = copyShard(shardname);
        System.out.println(shardCopy);

        final String[] stringFields = stringFieldNames;
        final String[] intFields = intFieldNames;
        System.out.println("string fields: \n" + Arrays.toString(stringFields));
        System.out.println("int fields: \n" + Arrays.toString(intFields));

        final MTImhotepLocalMultiSession verificationSession;
        verificationSession = createMultisession(Arrays.asList(shardCopy), metricNames);
        FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields);

        final MTImhotepLocalMultiSession testSession;
        testSession = createMultisession(Arrays.asList(shardname), metricNames);
        RunnableFactory factory = new WriteFTGSRunner(testSession, intFields, stringFields, 8);
        ClusterSimulator simulator = new ClusterSimulator(8, nMetrics);

        Thread t = simulator.simulateServer(factory);
        RawFTGSIterator ftgsIterator = simulator.getIterator();

        compareIterators(ftgsIterator, verificationIter, nMetrics);

        t.join();

        simulator.close();
        verificationIter.close();
    }

    @Test
    public void test10Shards() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
        //        final long seed = -4122356988999045667L;
        final long seed = 42;
        rand.setSeed(seed);
        System.out.println("Random seed: " + seed);
        final int numDocs = rand.nextInt(1 << 16) + 1;  // 64K

        // need at least one
        final int nIntFields = rand.nextInt(3 - 1) + 1;
        final int nStringFields = rand.nextInt(3 - 1) + 1;
        final int nMetrics = rand.nextInt(MAX_N_METRICS - 1) + 1;
        String[] stringFieldNames = new String[nStringFields];
        for (int i = 0; i < nStringFields; i++) {
            stringFieldNames[i] = String.format("str-%d", i);
        }
        String[] intFieldNames = new String[nIntFields];
        for (int i = 0; i < nIntFields; i++) {
            intFieldNames[i] = String.format("int-%d", i);
        }
        String[] metricNames = new String[nMetrics];
        for (int i = 0; i < nMetrics; i++) {
            metricNames[i] = String.format("metric-%d", i);
        }

        List<String> shardNames = new ArrayList<>();
        List<String> shardCopies = new ArrayList<>();
        final List<FieldDesc> fieldDescs =
                createShardProfile(numDocs,
                                   nMetrics,
                                   nStringFields,
                                   stringFieldNames,
                                   nIntFields,
                                   intFieldNames,
                                   metricNames,
                                   false);

        for (int i = 0; i < 10; i++) {
            final String shard = generateShard(fieldDescs);
            shardNames.add(shard);
            shardCopies.add(copyShard(shard));
        }

        final String[] stringFields = stringFieldNames;
        final String[] intFields = intFieldNames;

        final MTImhotepLocalMultiSession verificationSession;
        verificationSession = createMultisession(shardCopies, metricNames);
        FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields);

        final MTImhotepLocalMultiSession testSession;
        testSession = createMultisession(shardNames, metricNames);
        RunnableFactory factory = new WriteFTGSRunner(testSession, intFields, stringFields, 8);

        ClusterSimulator simulator = new ClusterSimulator(8, nMetrics);

        Thread t = simulator.simulateServer(factory);
        RawFTGSIterator ftgsIterator = simulator.getIterator();

        compareIterators(ftgsIterator, verificationIter, nMetrics);

        t.join();

        simulator.close();
        verificationIter.close();
    }

    private List<FieldDesc> createShardProfile(int numDocs,
                                               int nMetrics,
                                               int nStringFields,
                                               String[] stringFieldNames,
                                               int nIntFields,
                                               String[] intFieldNames,
                                               String[] metricNames,
                                               boolean binaryOnly) throws IOException {
        final List<FieldDesc> fieldDescs = new ArrayList<>(nMetrics);

        {
            // create string Fields
            for (int i = 0; i < nStringFields; i++) {
                final FieldDesc fd = new FieldDesc();
                fd.isIntfield = false;
                fd.name = stringFieldNames[i];
                fd.numDocs = numDocs;
                final int numTerms = 32;
                fd.terms = new ArrayList<>(numTerms);
                for (int j = 0; j < numTerms; j++) {
                    final String term = String.format("term-%d-xxx", j);
                    fd.terms.add(term);
                }
                fieldDescs.add(fd);
            }

            // create int Fields
            for (int i = 0; i < nIntFields; i++) {
                final FieldDesc fd = new FieldDesc();
                fd.isIntfield = true;
                fd.name = intFieldNames[i];
                fd.numDocs = numDocs;
                if (binaryOnly) {
                    fd.termGenerator = MetricMaxSizes.BINARY;
                } else {
                    fd.termGenerator = MetricMaxSizes.getRandomSize();
                }
                fieldDescs.add(fd);
            }
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

    private void dumpIterator(FTGSIterator it, int numStats) {
        while (it.nextField()) {
            System.err.println("field: " + it.fieldName());
            while (it.nextTerm()) {
                if (it.fieldIsIntType()) {
                    System.err.print("  term: " + it.termIntVal());
                } else {
                    System.err.print("  term: " + it.termStringVal());
                }
                while (it.nextGroup()) {
                    System.err.println("    group: " + it.group());
                    // !@# print stats...
                }
            }
        }
    }

    private void compareIterators(FTGSIterator test, FTGSIterator valid, int numStats) {
        final long[] validStats = new long[numStats];
        final long[] testStats = new long[numStats];

        while (valid.nextField()) {
            assertTrue(test.nextField());
            assertEquals(valid.fieldIsIntType(), test.fieldIsIntType());
            assertEquals(valid.fieldName(), test.fieldName());
            {
                final String v = valid.fieldName();
                final String t = test.fieldName();
                System.out.println("field - valid: " + v + ", test: " + t);
            }
            term: while (valid.nextTerm()) {
                assertTrue(test.nextTerm());
                assertEquals(valid.fieldIsIntType(), test.fieldIsIntType());
                if (valid.fieldIsIntType()) {
                    long v = valid.termIntVal();
                    final long t = test.termIntVal();
                    System.out.println("term - valid: " + v + ", test: " + t);
                    while (v != t) {
                        /* check to see if there are any groups in the term */
                        if (valid.nextGroup()) {
                            break;
                        }
                        if (!valid.nextTerm())
                            break term;
                        v = valid.termIntVal();
                    }
                    assertEquals(v, t);
                } else {
                    String v = valid.termStringVal();
                    final String t = test.termStringVal();
                    System.out.println("term - valid: " + v + ", test: " + t);
                    while (!v.equals(t)) {
                        /* check to see if there are any groups in the term */
                        if (valid.nextGroup()) {
                            break;
                        }
                        if (!valid.nextTerm())
                            break term;
                        v = valid.termStringVal();
                    }
                    assertEquals(v, t);
                }
//                assertEquals(valid.termDocFreq(), test.termDocFreq());
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

    private void readAllIterator(FTGSIterator iterator, int numStats) {
        final long[] validStats = new long[numStats];
        final long[] testStats = new long[numStats];

        while (iterator.nextField()) {
            while (iterator.nextTerm()) {
                if (iterator.fieldIsIntType()) {
                    final long v = iterator.termIntVal();
//                    System.out.println("valid: " + v + ", test: " + t);
                } else {
                    String v = iterator.termStringVal();
//                    System.out.println("valid: " + v + ", test: " + t);
                }
//                assertEquals(valid.termDocFreq(), test.termDocFreq());
                while (iterator.nextGroup()) {
//                    System.out.println(Integer.toString(valid.group()));
                    iterator.groupStats(validStats);
                }
            }
        }
    }
}
