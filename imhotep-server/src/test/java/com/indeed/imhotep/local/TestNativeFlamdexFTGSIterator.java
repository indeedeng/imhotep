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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.InputStreamFTGSIterator;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.RawFTGSMerger;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RawFTGSIterator;
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

    private static class UncheckedClusterSimulator {
        public static final int PORT = 8138;
        private ServerSocket serverSocket;
        private int nSplits;
        private int nStats;

        UncheckedClusterSimulator(int nSplits, int nStats) throws IOException {
            this.nSplits = nSplits;
            this.nStats = nStats;
            this.serverSocket = new ServerSocket(PORT);
        }

        public RawFTGSIterator getIterator() throws IOException {
            return null;
        }

        public Thread simulateServer(final RunnableFactory factory) throws IOException {
            final Thread result = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread[] threads = new Thread[nSplits];
                        for (int i = 0; i < nSplits; i++) {
                            Socket sock = null;
                            Thread t = new Thread(factory.newRunnable(sock, i));
                            t.start();
                            threads[i] = t;
                        }

                        for (Thread t : threads) {
                            t.join();
                        }
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

    private String generateShard(List<FieldDesc> fieldDescs) throws IOException {
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
                                                          String[] metricNames) throws ImhotepOutOfMemoryException, IOException {
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

        mtSession.randomMultiRegroup(metricNames[0],
                                     true,
                                     "12345",
                                     1,
                                     new double[] { 0.1, 0.4, 0.7 },
                                     new int[] { 0, 2, 3, 4 });

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
        final long seed = rand.nextLong();
//        final long seed = -2155255878033953833L;
        rand.setSeed(seed);
//        System.out.println("Random seed: " + seed);
        final int numDocs = rand.nextInt(1 << 16) + 1;  // 64K

        // need at least one
        final int nMetrics = rand.nextInt(MAX_N_METRICS - 1) + 1;
        String fieldName = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        String[] metricNames = new String[nMetrics];
        for (int i = 0; i < nMetrics; i++) {
            metricNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }

        final List<FieldDesc> fieldDescs = createShardProfile(numDocs, nMetrics, fieldName, metricNames);
        final String shardname = generateShard(fieldDescs);
//        System.out.println(shardname);
        String shardCopy = copyShard(shardname);
//        System.out.println(shardCopy);
//        final String shardname = "/tmp/native-ftgs-test3445570521338951695test";
//        final String shardCopy = "/tmp/native-ftgs-test5526201544532342896verify-copy";

        final String[] stringFields = new String[]{fieldName};
        final String[] intFields = new String[0];

        final MTImhotepLocalMultiSession verificationSession;
        verificationSession = createMultisession(Arrays.asList(shardCopy),
                                                 metricNames);
        FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields);

        final MTImhotepLocalMultiSession testSession;
        testSession = createMultisession(Arrays.asList(shardname),
                                           metricNames);
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
        final long seed = rand.nextLong();
//        final long seed = -4122356988999045667L;
        rand.setSeed(seed);
//        System.out.println("Random seed: " + seed);
        final int numDocs = rand.nextInt(1 << 16) + 1;  // 64K

        // need at least one
        final int nMetrics = rand.nextInt(MAX_N_METRICS - 1) + 1;
        String fieldName = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        String[] metricNames = new String[nMetrics];
        for (int i = 0; i < nMetrics; i++) {
            metricNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }

//        List<String> shardCopies =
//                Arrays.asList(new String[] { "/tmp/native-ftgs-test2019957327669565406verify-copy",
//                                            "/tmp/native-ftgs-test2086104149988911923verify-copy",
//                                            "/tmp/native-ftgs-test2550620506633025168verify-copy",
//                                            "/tmp/native-ftgs-test3378193281749620968verify-copy",
//                                            "/tmp/native-ftgs-test3521534428337709411verify-copy",
//                                            "/tmp/native-ftgs-test7398457726209096601verify-copy",
//                                            "/tmp/native-ftgs-test7624881454913744027verify-copy",
//                                            "/tmp/native-ftgs-test4957208100491061858verify-copy",
//                                            "/tmp/native-ftgs-test8643851881666654216verify-copy",
//                                            "/tmp/native-ftgs-test8771242174575689719verify-copy" });
//        List<String> shardNames =
//                Arrays.asList(new String[] { "/tmp/native-ftgs-test2516537865227324444test",
//                                            "/tmp/native-ftgs-test4939771279954664194test",
//                                            "/tmp/native-ftgs-test6735761589277623564test",
//                                            "/tmp/native-ftgs-test7045001985093560042test",
//                                            "/tmp/native-ftgs-test8960462730886152860test",
//                                            "/tmp/native-ftgs-test8988934566151064202test",
//                                            "/tmp/native-ftgs-test7741967259764742014test",
//                                            "/tmp/native-ftgs-test133381526348508656test",
//                                            "/tmp/native-ftgs-test4327346230585862211test",
//                                            "/tmp/native-ftgs-test2103214280390889017test" });
        List<String> shardNames = new ArrayList<>();
        List<String> shardCopies = new ArrayList<>();
        final List<FieldDesc> fieldDescs = createShardProfile(numDocs,
                                                             nMetrics,
                                                             fieldName,
                                                             metricNames);
        for (int i = 0; i < 10; i++) {
            final String shard = generateShard(fieldDescs);
            shardNames.add(shard);
            shardCopies.add(copyShard(shard));
        }

        final String[] stringFields = new String[]{fieldName};
        final String[] intFields = new String[0];

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
//                    System.out.println("valid: " + v + ", test: " + t);
                    assertEquals(v, t);
                } else {
                    String v = valid.termStringVal();
                    final String t = test.termStringVal();
//                    System.out.println("valid: " + v + ", test: " + t);
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
//                assertEquals(valid.termDocFreq(), test.termDocFreq());
                while (valid.nextGroup()) {
//                    System.out.println(Integer.toString(valid.group()));
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
