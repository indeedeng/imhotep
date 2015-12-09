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

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.indeed.flamdex.api.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.*;
import org.junit.rules.*;

import com.indeed.flamdex.simple.*;
import com.indeed.flamdex.writer.*;
import com.indeed.imhotep.*;
import com.indeed.imhotep.api.*;
import com.indeed.util.io.Files;

/**
 * @author darren
 */
public class TestNativeFlamdexFTGSIterator {

    @Rule public Timeout globalTimeout = new Timeout(120000);

    private static String rootTestDir;

    @BeforeClass public static void setUp() throws IOException {
        rootTestDir = Files.getTempDirectory("rootTestDir.", ".delete.me");
    }

    @AfterClass public static void tearDown() {
        Files.delete(rootTestDir);
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

    private ArrayList<String> stringTerms;
    private final Random rand = new Random();
    private static final int MAX_STRING_TERM_LEN = 128;

    public TestNativeFlamdexFTGSIterator() {
        final int numTerms = (1 << 12);  // 4K
        stringTerms = new ArrayList<String>(numTerms);

        StringBuilder prevStr = new StringBuilder(randomString(rand.nextInt(MAX_STRING_TERM_LEN / 2) + 1));
        for (int j = 0; j < numTerms; j++) {
            boolean back = rand.nextBoolean();
            int numForward = rand.nextInt(20);
            if (back) {
                int numBack = rand.nextInt(20);
                numBack = (numBack > prevStr.length()) ? prevStr.length() : numBack;
                prevStr.setLength(prevStr.length() - numBack);
            }
            prevStr.append(randomString(numForward));
            stringTerms.add(prevStr.toString());
        }

        Collections.sort(stringTerms);
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

        final UnusedDocsIds unusedDocIds = new UnusedDocsIds(rand, size);

        TreeMap<Long, int[]> map = new TreeMap<>();
        while (unusedDocIds.size() > maxUnassignedDocs) {
            final long term = termGenerator.randVal();
            if (term == 0 && termGenerator != MetricMaxSizes.BINARY) {
                continue;
            }
            final int maxDocsPerTerm = Math.min(unusedDocIds.size(), maxTermDocs);
            final int numDocs = unusedDocIds.size() > 1 ? rand.nextInt(maxDocsPerTerm - 1) + 1 : 1;
            final int[] docIds = selectDocIds(unusedDocIds, numDocs);
            final int[] existingDocs = map.get(term);
            if (existingDocs == null)
                map.put(term, docIds);
            else {
                int[] arr = new int[docIds.length + existingDocs.length];
                System.arraycopy(docIds, 0, arr, 0, docIds.length);
                System.arraycopy(existingDocs, 0, arr, docIds.length, existingDocs.length);
                Arrays.sort(arr);
                map.put(term, arr);
            }
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

        final UnusedDocsIds unusedDocIds = new UnusedDocsIds(rand, size);

        int i = 0;
        TreeMap<String, int[]> map = new TreeMap<>();
        while (unusedDocIds.size() > maxUnassignedDocs) {
            final String term = termsList.get(i);

            final int maxDocsPerTerm = Math.min(unusedDocIds.size(), maxTermDocs);
            final int numDocs = unusedDocIds.size() > 1 ? rand.nextInt(maxDocsPerTerm - 1) + 1 : 1;
            final int[] docIds = selectDocIds(unusedDocIds, numDocs);
            map.put(term, docIds);
            i++;
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
        final String dir = Files.getTempDirectory("native-ftgs-test", "test", rootTestDir);

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
        final String new_dir = Files.getTempDirectory("native-ftgs-test", "verify-copy", rootTestDir);

        FileUtils.copyDirectory(new File(dir), new File(new_dir));

        return new_dir;
    }

    public static class NotRandom {
        private int num;

        private NotRandom(int n) { this.num = n; }

        public static NotRandom closed(int begin, int end) {
            return new NotRandom(begin);
        }

        public long nextLong() {
            return num++;
        }

        public long nextInt() {
            return num++;
        }

        public void nextBytes(byte[] b) {
            b[0] = (byte)num++;
        }

        public boolean nextBoolean() {
            return (num++ & 1) == 0;
        }

        public int nextInt(int i) {
            return num++ % i;
        }


    }


    private static final int MAX_N_METRICS = 64;

    class ImhotepLocalSessionFactory {
        public ImhotepLocalSession create(SimpleFlamdexReader reader)
            throws ImhotepOutOfMemoryException {
            return new ImhotepJavaLocalSession(reader);
        }
    }

    class ImhotepNativeLocalSessionFactory extends ImhotepLocalSessionFactory {
        @Override
        public ImhotepLocalSession create(SimpleFlamdexReader reader)
            throws ImhotepOutOfMemoryException {
            return new ImhotepNativeLocalSession(reader);
        }
    }

    private MTImhotepLocalMultiSession createMultisession(List<String> shardDirs,
                                                          String[] intFieldNames,
                                                          String[] strFieldNames,
                                                          String[] metricNames,
                                                          ImhotepLocalSessionFactory factory)
        throws ImhotepOutOfMemoryException, IOException {
        ImhotepLocalSession[] localSessions = new ImhotepLocalSession[shardDirs.size()];
        for (int i = 0; i < shardDirs.size(); i++) {
            final String dir = shardDirs.get(i);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            final ImhotepLocalSession localSession;
            localSession = factory.create(r);
            localSessions[i] = localSession;
        }

        AtomicLong foo = new AtomicLong(1000000000);
        final MTImhotepLocalMultiSession mtSession;
        mtSession = new MTImhotepLocalMultiSession(localSessions,
                                                   new MemoryReservationContext(new ImhotepMemoryPool(
                                                           Long.MAX_VALUE)),
                                                   foo, true);

        String regroupField = (intFieldNames.length >= 1) ? intFieldNames[1] : strFieldNames[1];
        mtSession.randomMultiRegroup(regroupField,
                                     true,
                                     "12345",
                                     1,
                                     new double[] { 0.1, 0.4, 0.7 },
                                     new int[] { 1, 2, 3, 4 });

//        System.out.println(Arrays.toString(metricNames));
        for (String metric : metricNames) {
            mtSession.pushStat(metric);
        }
        mtSession.popStat();
        mtSession.pushStat("count()");

        return mtSession;
    }

    private String randomString(int len) {
        return RandomStringUtils.random(len, 0, 0, true, false, null, rand);
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
                final int numTerms = (1 << 12);  // 1M
                fd.terms = new ArrayList<>(numTerms);
                for (int j = 0; j < numTerms; j++) {
                    fd.terms.add(stringTerms.get(j));
                }
                fieldDescs.add(fd);
            }

            // create int Fields
            for (int i = 0; i < nIntFields; i++) {
                final FieldDesc fd = new FieldDesc();
                fd.isIntfield = true;
                fd.name = intFieldNames[i];
                fd.numDocs = numDocs;
                fd.termGenerator = MetricMaxSizes.getRandomSize();
                fieldDescs.add(fd);
            }
        }

        // create metrics
        for (int i = 0; i < nMetrics; i++) {
            final FieldDesc fd = new FieldDesc();
            fd.isIntfield = true;
            fd.name = metricNames[i];
            if (binaryOnly) {
                fd.termGenerator = MetricMaxSizes.BINARY;
            } else {
                fd.termGenerator = MetricMaxSizes.getRandomSize();
            }
            fd.numDocs = numDocs;
            fieldDescs.add(fd);
        }

        return fieldDescs;
    }

    @Test
    public void testSingleShardBinary() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
        final long seed = rand.nextLong();
//        final long seed = 6675433312916519811L;
        rand.setSeed(seed);
        System.out.println("Random seed: " + seed);
        final int numDocs = (1 << 17) + rand.nextInt(1 << 17) + 1;  // 64K

        // need at least one
        final int nMetrics = 2;
        final int nIntFields = rand.nextInt(4 - 1) + 3;
        final int nStringFields = rand.nextInt(4 - 1) + 3;
//        final int nStringFields = 0;
        String[] stringFieldNames = new String[nStringFields];
        for (int i = 0; i < nStringFields; i++) {
            stringFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
        }
        String[] intFieldNames = new String[nIntFields];
        for (int i = 0; i < nIntFields; i++) {
            intFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
        }
        String[] metricNames = new String[nMetrics];
        for (int i = 0; i < nMetrics; i++) {
            metricNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
        }

        final List<FieldDesc> fieldDescs = createShardProfile(numDocs,
                                                              nMetrics,
                                                              nStringFields,
                                                              stringFieldNames,
                                                              nIntFields,
                                                              intFieldNames,
                                                              metricNames,
                                                              true);

        List<String> shardNames = new ArrayList<>();
        List<String> shardCopies = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final String shard = generateShard(fieldDescs);
            shardNames.add(shard);
            System.out.println(shard);
            shardCopies.add(copyShard(shard));
        }
//        List<String> shardCopies =
//                Arrays.asList(new String[] { "/tmp/native-ftgs-test1754672327590107058test",
//                                            "/tmp/native-ftgs-test5801028131674359035test",
//                                            "/tmp/native-ftgs-test693034998642167321test",
//                                            "/tmp/native-ftgs-test4646832364786817065test",
//                                            "/tmp/native-ftgs-test4049390234829335463test"});
//        List<String> shardNames =
//                Arrays.asList(new String[] { "/tmp/native-ftgs-test1754672327590107058test",
//                                             "/tmp/native-ftgs-test5801028131674359035test",
//                                             "/tmp/native-ftgs-test693034998642167321test",
//                                             "/tmp/native-ftgs-test4646832364786817065test",
//                                             "/tmp/native-ftgs-test4049390234829335463test"});

        final String[] stringFields = stringFieldNames;
        final String[] intFields = intFieldNames;
        System.out.println("string fields: \n" + Arrays.toString(stringFields));
        System.out.println("int fields: \n" + Arrays.toString(intFields));

        final MTImhotepLocalMultiSession verificationSession;
        verificationSession =
                createMultisession(shardCopies,
                                   intFieldNames,
                                   stringFieldNames,
                                   metricNames,
                                   new ImhotepLocalSessionFactory());
        FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields);

        final MTImhotepLocalMultiSession testSession;
        testSession =
                createMultisession(shardNames,
                                   intFieldNames,
                                   stringFieldNames,
                                   metricNames,
                                   new ImhotepNativeLocalSessionFactory());
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
    public void testSingleShard() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
        final long seed = rand.nextLong();
//        final long seed = 6757079647648910486L;
        rand.setSeed(seed);
        System.out.println("Random seed: " + seed);
        final int numDocs = rand.nextInt(1 << 17) + 1;  // 64K

        // need at least one
        final int nMetrics = rand.nextInt(MAX_N_METRICS - 1) + 1;
        final int nIntFields = rand.nextInt(3 - 1) + 2;
        final int nStringFields = rand.nextInt(3 - 1) + 2;
        String[] stringFieldNames = new String[nStringFields];
        for (int i = 0; i < nStringFields; i++) {
            stringFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }
        String[] intFieldNames = new String[nIntFields];
        for (int i = 0; i < nIntFields; i++) {
            intFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }
        String[] metricNames = new String[nMetrics];
        for (int i = 0; i < nMetrics; i++) {
            metricNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }

        final List<FieldDesc> fieldDescs =
                createShardProfile(numDocs,
                                   nMetrics,
                                   nStringFields,
                                   stringFieldNames,
                                   nIntFields,
                                   intFieldNames,
                                   metricNames,
                                   false);
        final String shardname = generateShard(fieldDescs);
        System.out.println(shardname);
        String shardCopy = copyShard(shardname);
        System.out.println(shardCopy);
//        final String shardname = "/tmp/native-ftgs-test8719739701043085471test";
//        final String shardCopy = "/tmp/native-ftgs-test536071247070284384verify-copy";

        final String[] stringFields = stringFieldNames;
        final String[] intFields = intFieldNames;
        System.out.println("string fields: \n" + Arrays.toString(stringFields));
        System.out.println("int fields: \n" + Arrays.toString(intFields));

        final MTImhotepLocalMultiSession verificationSession;
        verificationSession =
                createMultisession(Arrays.asList(shardCopy),
                                   intFieldNames,
                                   stringFieldNames,
                                   metricNames,
                                   new ImhotepLocalSessionFactory());
        FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields);

        final MTImhotepLocalMultiSession testSession;
        testSession =
                createMultisession(Arrays.asList(shardname),
                                   intFieldNames,
                                   stringFieldNames,
                                   metricNames,
                                   new ImhotepNativeLocalSessionFactory());
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
//        final long seed = 5934453071102157952L;
        rand.setSeed(seed);
        System.out.println("Random seed: " + seed);
        final int numDocs = rand.nextInt(1 << 17) + 1;  // 64K

        // need at least one
        final int nIntFields = rand.nextInt(3 - 1) + 2;
        final int nStringFields = rand.nextInt(3 - 1) + 2;
        final int nMetrics = rand.nextInt(MAX_N_METRICS - 1) + 1;
        String[] stringFieldNames = new String[nStringFields];
        for (int i = 0; i < nStringFields; i++) {
            stringFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }
        String[] intFieldNames = new String[nIntFields];
        for (int i = 0; i < nIntFields; i++) {
            intFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }
        String[] metricNames = new String[nMetrics];
        for (int i = 0; i < nMetrics; i++) {
            metricNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }

//        List<String> shardCopies =
//                Arrays.asList(new String[] { "/tmp/native-ftgs-test4073440857071201612test",
//                                            "/tmp/native-ftgs-test3810231838030567063test",
//                                            "/tmp/native-ftgs-test1648119374588231616test",
//                                            "/tmp/native-ftgs-test1120994615092318961test",
//                                            "/tmp/native-ftgs-test602330686619554371test",
//                                            "/tmp/native-ftgs-test4229417530299664328test",
//                                            "/tmp/native-ftgs-test3602804376498486243test",
//                                            "/tmp/native-ftgs-test4175013008290753425test",
//                                            "/tmp/native-ftgs-test3900578412908250666test",
//                                            "/tmp/native-ftgs-test3495740989105538049test" });
//        List<String> shardNames =
//                Arrays.asList(new String[] { "/tmp/native-ftgs-test4073440857071201612test",
//                                           "/tmp/native-ftgs-test3810231838030567063test",
//                                           "/tmp/native-ftgs-test1648119374588231616test",
//                                           "/tmp/native-ftgs-test1120994615092318961test",
//                                           "/tmp/native-ftgs-test602330686619554371test",
//                                           "/tmp/native-ftgs-test4229417530299664328test",
//                                           "/tmp/native-ftgs-test3602804376498486243test",
//                                           "/tmp/native-ftgs-test4175013008290753425test",
//                                           "/tmp/native-ftgs-test3900578412908250666test",
//                                           "/tmp/native-ftgs-test3495740989105538049test" });
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
            System.out.println(shard);
            shardCopies.add(copyShard(shard));
        }

        final String[] stringFields = stringFieldNames;
        final String[] intFields = intFieldNames;
        System.out.println("string fields: \n" + Arrays.toString(stringFields));
        System.out.println("int fields: \n" + Arrays.toString(intFields));

        final MTImhotepLocalMultiSession verificationSession;
        verificationSession =
                createMultisession(shardCopies,
                                   intFieldNames,
                                   stringFieldNames,
                                   metricNames,
                                   new ImhotepLocalSessionFactory());
        FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields);

        final MTImhotepLocalMultiSession testSession;
        testSession =
                createMultisession(shardNames,
                                   intFieldNames,
                                   stringFieldNames,
                                   metricNames,
                                   new ImhotepNativeLocalSessionFactory());
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
    public void testNativeMetricInverter() throws IOException, FlamdexOutOfMemoryException {
        final long seed = rand.nextLong();
//        final long seed = 5934453071102157952L;
        rand.setSeed(seed);
        System.out.println("Random seed: " + seed);
        final int numDocs = (1 << 17) + rand.nextInt(1 << 17)+ 1;  // 128K

        final int nMetrics = MetricMaxSizes.values().length;
        String[] metricNames = new String[nMetrics];
        for (int i = 0; i < nMetrics; i++) {
            metricNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
        }

//        List<String> shardCopies =
//                Arrays.asList(new String[] { "/tmp/native-ftgs-test4073440857071201612test",
//                                            "/tmp/native-ftgs-test3495740989105538049test" });
//        List<String> shardNames =
//                Arrays.asList(new String[] { "/tmp/native-ftgs-test4073440857071201612test",
//                                           "/tmp/native-ftgs-test3495740989105538049test" });

//        final List<FieldDesc> fieldDescs = createShardProfile(numDocs,
//                                                              nMetrics,
//                                                              0,
//                                                              new String[0],
//                                                              0,
//                                                              new String[0],
//                                                              metricNames,
//                                                              false);

        // create metrics
        final List<FieldDesc> fieldDescs = new ArrayList<>(nMetrics);
        int j = 0;
        for (MetricMaxSizes sz : MetricMaxSizes.values()) {
            final FieldDesc fd = new FieldDesc();
            fd.isIntfield = true;
            fd.name = metricNames[j++];
            fd.termGenerator = sz;
            fd.numDocs = numDocs;
            fieldDescs.add(fd);
        }

        final String shardName = generateShard(fieldDescs);
        System.out.println(shardName);
        final String shardCopy = copyShard(shardName);

        final SimpleFlamdexReader verificationShard = SimpleFlamdexReader.open(shardCopy);
        final SimpleFlamdexReader testShard = SimpleFlamdexReader.open(shardName);
        final int nDocs = testShard.getNumDocs();

        // jit the code
        long foo = 0;
        int d[] = new int[1];
        long v[] = new long[1];
        d[0] = nDocs - 3;
        for (int i = 0; i < 100; i++) {
            for (String metric : metricNames) {
                final IntValueLookup verificationIVL = verificationShard.getMetricJava(metric);
                verificationIVL.lookup(d, v, 1);
                foo += v[0];
                final IntValueLookup testIVL = testShard.getMetric(metric);
                testIVL.lookup(d, v, 1);
                foo += v[0];
            }
        }
        System.out.println("foo: " + foo);

        for (String metric : metricNames) {
            final long t1 = System.nanoTime();
            final IntValueLookup verificationIVL = verificationShard.getMetricJava(metric);
            final long t2 = System.nanoTime();
            final IntValueLookup testIVL = testShard.getMetric(metric);
            final long t3 = System.nanoTime();
            System.out.println("Timings (max " + testIVL.getMax() + ") - "
                                       + " java: " + (t2 - t1)
                                       + " native: " + (t3 - t2));

            int offset = 0;
            int[] docIds = new int[1024];
            long[] goodValues = new long[1024];
            long[] testValues = new long[1024];
            do {
                int count = (nDocs - offset > 1024) ? 1024 : nDocs - offset;
                for (int i = 0; i < 1024; i++) {
                    docIds[i] = i + offset;
                }
                verificationIVL.lookup(docIds, goodValues, count);
                testIVL.lookup(docIds, testValues, count);
                assertArrayEquals("inverted metrics do not match in block " + ((offset / 1024) + 1),
                                  goodValues, testValues);
                offset += count;
            } while(offset < nDocs);
        }


    }

    private void compareIterators(FTGSIterator test, FTGSIterator valid, int numStats) {
        final long[] validStats = new long[numStats];
        final long[] testStats = new long[numStats];
        final long[] zeroStats = new long[numStats];

        while (valid.nextField()) {
            assertTrue(test.nextField());
            assertEquals(valid.fieldIsIntType(), test.fieldIsIntType());
            assertEquals(valid.fieldName(), test.fieldName());
            {
                final String v = valid.fieldName();
                final String t = test.fieldName();
                System.out.println("field - valid: " + v + ", test: " + t);
            }
            int i = 1;
            int tmp = 0;
            term: while (valid.nextTerm()) {
                assertTrue("nextTerm does not match.  iteration " + i, test.nextTerm());
                i++;
                assertEquals(valid.fieldIsIntType(), test.fieldIsIntType());
                if (valid.fieldIsIntType()) {
                    long v = valid.termIntVal();
                    final long t = test.termIntVal();
//                    System.out.println("int term - valid: " + v + ", test: " + t);
                    while (v != t) {
                        /* check to see if there are any groups in the term */
                        if (valid.nextGroup()) {
                            System.out.println("HI MOM! " + tmp++ + "\n");
                            break;
                        }
                        if (!valid.nextTerm()) {
                            System.out.println("HI MOM! " + tmp++ + "\n");
                            break term;
                        }
                        v = valid.termIntVal();
                    }
                    assertEquals(v, t);
                } else {
                    String v = valid.termStringVal();
                    final String t = test.termStringVal();
//                    System.out.println("string term - valid: " + v + ", test: " + t);
                    while (!v.equals(t)) {
                        /* check to see if there are any groups in the term */
                        if (valid.nextGroup()) {
                            System.out.println("HI MOM! " + tmp++ + "\n");
                            break;
                        }
                        if (!valid.nextTerm()) {
                            System.out.println("HI MOM! " + tmp++ + "\n");
                            break term;
                        }
                        v = valid.termStringVal();
                    }
                    assertEquals(v, t);
                }
                long validFreq = valid.termDocFreq();
                long testFreq = test.termDocFreq();
//                System.out.println("Doc Frequency - valid: " + validFreq + ", test: " + testFreq);
                assertEquals("Doc Freqs do not match.  iter " + i, validFreq, testFreq);
                while (valid.nextGroup()) {
                    assertTrue(test.nextGroup());
                    assertEquals(valid.group(), test.group());

                    valid.groupStats(validStats);
                    test.groupStats(testStats);
                    if (Arrays.equals(zeroStats, testStats)) {
                        System.out.println("i: " + i + " Term: " + valid.termIntVal() + " Group: " + Integer.toString(valid.group()));
                        System.out.println("Group stats. valid: " + Arrays.toString(validStats) + " test: " + Arrays.toString(testStats));

                    }
                    assertArrayEquals("Group stats do not match. valid: " +
                                        Arrays.toString(validStats) +
                                        " test: " + Arrays.toString(testStats),
                                        validStats,testStats);
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
