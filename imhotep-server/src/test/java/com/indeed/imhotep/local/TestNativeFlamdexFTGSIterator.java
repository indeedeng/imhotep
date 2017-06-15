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

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.IntValueLookup;
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
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.io.TestFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author darren
 */
public class TestNativeFlamdexFTGSIterator {

    @Rule
    public final TemporaryFolder rootTestDir = new TemporaryFolder();

    public static class ClusterSimulator {
        private static final int PORT = 8138;
        private final ServerSocket serverSocket;
        private final int nSplits;
        private final int nStats;

        ClusterSimulator(final int nSplits, final int nStats) throws IOException {
            this.nSplits = nSplits;
            this.nStats = nStats;
            this.serverSocket = new ServerSocket(PORT);
        }

        public RawFTGSIterator getIterator() throws IOException {
            final List<RawFTGSIterator> iters = new ArrayList<>();
            for (int i = 0; i < nSplits; i++) {
                final Socket sock = new Socket("localhost", PORT);
                final InputStream is = sock.getInputStream();
                final RawFTGSIterator ftgs = new InputStreamFTGSIterator(is, nStats);
                iters.add(ftgs);
            }
            final RawFTGSMerger merger  = new RawFTGSMerger(iters, nStats, null);
            return merger;
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

    public static class WriteFTGSRunner implements RunnableFactory {
        private final MTImhotepLocalMultiSession session;
        private final String[] intFields;
        private final String[] stringFields;
        private final int numSplits;

        public WriteFTGSRunner(final MTImhotepLocalMultiSession session,
                               final String[] intFields,
                               final String[] stringFields,
                               final int numSplits) {
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
                    } catch (final IOException | ImhotepOutOfMemoryException e) {
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

    private final List<String> stringTerms;
    private final Random rand = new Random();
    private static final int MAX_STRING_TERM_LEN = 128;

    public TestNativeFlamdexFTGSIterator() {
        final int numTerms = (1 << 12);  // 4K
        stringTerms = new ArrayList<String>(numTerms);

        final StringBuilder prevStr = new StringBuilder(randomString(rand.nextInt(MAX_STRING_TERM_LEN / 2) + 1));
        for (int j = 0; j < numTerms; j++) {
            final boolean back = rand.nextBoolean();
            final int numForward = rand.nextInt(20);
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
                                       final SimpleFlamdexWriter w,
                                       final long maxDocs) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        final int maxUnassignedDocs = (size < 100) ? 10 : 100;
        final IntFieldWriter ifw = w.getIntFieldWriter(intFieldName);

        final UnusedDocsIds unusedDocIds = new UnusedDocsIds(rand, size);

        final TreeMap<Long, int[]> map = new TreeMap<>();
        while (unusedDocIds.size() > maxUnassignedDocs) {
            final long term = termGenerator.randVal();
            if (term == 0 && termGenerator != MetricMaxSizes.BINARY) {
                continue;
            }
            final int maxDocsPerTerm = Math.min(unusedDocIds.size(), maxTermDocs);
            final int numDocs = unusedDocIds.size() > 1 ? rand.nextInt(maxDocsPerTerm - 1) + 1 : 1;
            final int[] docIds = selectDocIds(unusedDocIds, numDocs);
            final int[] existingDocs = map.get(term);
            if (existingDocs == null) {
                map.put(term, docIds);
            } else {
                final int[] arr = new int[docIds.length + existingDocs.length];
                System.arraycopy(docIds, 0, arr, 0, docIds.length);
                System.arraycopy(existingDocs, 0, arr, docIds.length, existingDocs.length);
                Arrays.sort(arr);
                map.put(term, arr);
            }
        }

        for (final Long term : map.keySet()) {
            ifw.nextTerm(term);
            for (final int doc : map.get(term)) {
                if (doc >= maxDocs) {
                    throw new RuntimeException("WTF!");
                }
                ifw.nextDoc(doc);
            }
        }
        ifw.close();
    }

    private void createFlamdexStringField(final String stringFieldName,
                                          final int size,
                                          final SimpleFlamdexWriter w,
                                          final List<String> termsList,
                                          final long maxDocs) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        final int maxUnassignedDocs = (size < 100) ? 10 : 100;
        final StringFieldWriter sfw = w.getStringFieldWriter(stringFieldName);

        final UnusedDocsIds unusedDocIds = new UnusedDocsIds(rand, size);

        int i = 0;
        final TreeMap<String, int[]> map = new TreeMap<>();
        while (unusedDocIds.size() > maxUnassignedDocs) {
            final String term = termsList.get(i);

            final int maxDocsPerTerm = Math.min(unusedDocIds.size(), maxTermDocs);
            final int numDocs = unusedDocIds.size() > 1 ? rand.nextInt(maxDocsPerTerm - 1) + 1 : 1;
            final int[] docIds = selectDocIds(unusedDocIds, numDocs);
            map.put(term, docIds);
            i++;
        }

        for (final String term : map.keySet()) {
            sfw.nextTerm(term);
            for (final int doc : map.get(term)) {
                if (doc >= maxDocs) {
                    throw new RuntimeException("WTF!");
                }
                sfw.nextDoc(doc);
            }
        }
        sfw.close();
    }

    private Path generateShard(final List<FieldDesc> fieldDescs) throws IOException {
        return generateShard(fieldDescs, 0);
    }

    private Path generateShard(final List<FieldDesc> fieldDescs, final int index) throws IOException {

        if( index < 0 || index >= 24 ) {
            // we expect only small indexes
            throw new IllegalArgumentException("TestNativeFlamdexFTGSIterator::generateShard - wrong index");
        }

        final DateTime shardDate = new DateTime(2017, 1, 1, index, 0, 0 );
        final Path dir = TestFileUtils.createTempShard(rootTestDir.getRoot().toPath(), shardDate, ".native-ftgs-test.test-dir.");
        // find max # of docs
        long nDocs = 0;
        for (final FieldDesc fd : fieldDescs) {
            nDocs = (fd.numDocs > nDocs) ? fd.numDocs : nDocs;
        }

//        System.out.println("numdocs: " + nDocs);

        final SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, nDocs, true);

        for (final FieldDesc fd : fieldDescs) {
            if (fd.isIntfield) {
                createFlamdexIntField(fd.name, fd.termGenerator, fd.numDocs, w, nDocs);
            } else {
                createFlamdexStringField(fd.name, fd.numDocs, w, fd.terms, nDocs);
            }
        }

        w.close();
        return dir;
    }

    private Path copyShard(final Path dir) throws IOException {

        final String srcShardName = dir.getFileName().toString();
        final DateTime shardTime = ShardTimeUtils.parseStart(srcShardName);
        final Path newDir = TestFileUtils.createTempShard(rootTestDir.getRoot().toPath(), shardTime, ".native-ftgs-test.verify-dir.");

        FileUtils.copyDirectory(dir.toFile(), newDir.toFile());

        return newDir;
    }

    private static final int MAX_N_METRICS = 64;

    class ImhotepLocalSessionFactory {
        public ImhotepLocalSession create(final SimpleFlamdexReader reader)
            throws ImhotepOutOfMemoryException {
            return new ImhotepJavaLocalSession(reader);
        }
    }

    class ImhotepNativeLocalSessionFactory extends ImhotepLocalSessionFactory {
        @Override
        public ImhotepLocalSession create(final SimpleFlamdexReader reader)
            throws ImhotepOutOfMemoryException {
            return new ImhotepNativeLocalSession(reader);
        }
    }

    private MTImhotepLocalMultiSession createMultisession(final List<Path> shardDirs,
                                                          final String[] intFieldNames,
                                                          final String[] strFieldNames,
                                                          final String[] metricNames,
                                                          final ImhotepLocalSessionFactory factory)
        throws ImhotepOutOfMemoryException, IOException {
        final ImhotepLocalSession[] localSessions = new ImhotepLocalSession[shardDirs.size()];
        for (int i = 0; i < shardDirs.size(); i++) {
            final Path dir = shardDirs.get(i);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            final ImhotepLocalSession localSession;
            localSession = factory.create(r);
            localSessions[i] = localSession;
        }

        final AtomicLong foo = new AtomicLong(1000000000);
        final MTImhotepLocalMultiSession mtSession;
        mtSession = new MTImhotepLocalMultiSession(localSessions,
                                                   new MemoryReservationContext(new ImhotepMemoryPool(
                                                           Long.MAX_VALUE)),
                                                   foo, true);

        final String regroupField = (intFieldNames.length >= 1) ? intFieldNames[1] : strFieldNames[1];
        mtSession.randomMultiRegroup(regroupField,
                                     true,
                                     "12345",
                                     1,
                                     new double[] { 0.1, 0.4, 0.7 },
                                     new int[] { 1, 2, 3, 4 });

//        System.out.println(Arrays.toString(metricNames));
        for (final String metric : metricNames) {
            mtSession.pushStat(metric);
        }
        mtSession.popStat();
        mtSession.pushStat("count()");

        return mtSession;
    }

    private String randomString(final int len) {
        return RandomStringUtils.random(len, 0, 0, true, false, null, rand);
    }

    private List<FieldDesc> createShardProfile(final int numDocs,
                                               final int nMetrics,
                                               final int nStringFields,
                                               final String[] stringFieldNames,
                                               final int nIntFields,
                                               final String[] intFieldNames,
                                               final String[] metricNames,
                                               final boolean binaryOnly) throws IOException {
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
        final String[] stringFieldNames = new String[nStringFields];
        for (int i = 0; i < nStringFields; i++) {
            stringFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
        }
        final String[] intFieldNames = new String[nIntFields];
        for (int i = 0; i < nIntFields; i++) {
            intFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN - 1) + 1);
        }
        final String[] metricNames = new String[nMetrics];
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

        final List<Path> shardNames = new ArrayList<>();
        final List<Path> shardCopies = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final Path shard = generateShard(fieldDescs, i);
            shardNames.add(shard);
//            System.out.println(shard);
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

        ClusterSimulator simulator = null;
        try (
            MTImhotepLocalMultiSession verificationSession = createMultisession(shardCopies,
                intFieldNames,
                stringFieldNames,
                metricNames,
                new ImhotepLocalSessionFactory());
            MTImhotepLocalMultiSession testSession = createMultisession(shardNames,
                intFieldNames,
                stringFieldNames,
                metricNames,
                new ImhotepNativeLocalSessionFactory());
            FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields) )
        {
            final RunnableFactory factory = new WriteFTGSRunner(testSession, intFields, stringFields, 8);

            simulator = new ClusterSimulator(8, nMetrics);
            final Thread t = simulator.simulateServer(factory);
            final RawFTGSIterator ftgsIterator = simulator.getIterator();

            compareIterators(ftgsIterator, verificationIter, nMetrics);

            t.join();

        } finally {
            if( simulator != null ) {
                simulator.close();
            }
        }
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
        final String[] stringFieldNames = new String[nStringFields];
        for (int i = 0; i < nStringFields; i++) {
            stringFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }
        final String[] intFieldNames = new String[nIntFields];
        for (int i = 0; i < nIntFields; i++) {
            intFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }
        final String[] metricNames = new String[nMetrics];
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
        final Path shardname = generateShard(fieldDescs);
        System.out.println(shardname);
        final Path shardCopy = copyShard(shardname);
        System.out.println(shardCopy);
//        final String shardname = "/tmp/native-ftgs-test8719739701043085471test";
//        final String shardCopy = "/tmp/native-ftgs-test536071247070284384verify-copy";

        final String[] stringFields = stringFieldNames;
        final String[] intFields = intFieldNames;
        System.out.println("string fields: \n" + Arrays.toString(stringFields));
        System.out.println("int fields: \n" + Arrays.toString(intFields));


        ClusterSimulator simulator = null;
        try(
            MTImhotepLocalMultiSession verificationSession = createMultisession(
                Arrays.asList(shardCopy),
                intFieldNames,
                stringFieldNames,
                metricNames,
                new ImhotepLocalSessionFactory());

            MTImhotepLocalMultiSession testSession = createMultisession(
                Arrays.asList(shardname),
                intFieldNames,
                stringFieldNames,
                metricNames,
                new ImhotepNativeLocalSessionFactory());
            FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields) )
        {
            final RunnableFactory factory = new WriteFTGSRunner(testSession, intFields, stringFields, 8);
            simulator = new ClusterSimulator(8, nMetrics);

            final Thread t = simulator.simulateServer(factory);
            final RawFTGSIterator ftgsIterator = simulator.getIterator();

            compareIterators(ftgsIterator, verificationIter, nMetrics);

            t.join();
        } finally {
            if(simulator != null) {
                simulator.close();
            }
        }
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
        final String[] stringFieldNames = new String[nStringFields];
        for (int i = 0; i < nStringFields; i++) {
            stringFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }
        final String[] intFieldNames = new String[nIntFields];
        for (int i = 0; i < nIntFields; i++) {
            intFieldNames[i] = randomString(rand.nextInt(MAX_STRING_TERM_LEN-1) + 1);
        }
        final String[] metricNames = new String[nMetrics];
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
        final List<Path> shardNames = new ArrayList<>();
        final List<Path> shardCopies = new ArrayList<>();
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
            final Path shard = generateShard(fieldDescs);
            shardNames.add(shard);
            System.out.println(shard);
            shardCopies.add(copyShard(shard));
        }

        final String[] stringFields = stringFieldNames;
        final String[] intFields = intFieldNames;
        System.out.println("string fields: \n" + Arrays.toString(stringFields));
        System.out.println("int fields: \n" + Arrays.toString(intFields));

        ClusterSimulator simulator = null;
        try (
            MTImhotepLocalMultiSession verificationSession = createMultisession(
                shardCopies,
                intFieldNames,
                stringFieldNames,
                metricNames,
                new ImhotepLocalSessionFactory());
            MTImhotepLocalMultiSession testSession = createMultisession(
                shardNames,
                intFieldNames,
                stringFieldNames,
                metricNames,
                new ImhotepNativeLocalSessionFactory());
            FTGSIterator verificationIter = verificationSession.getFTGSIterator(intFields, stringFields) ) {
            final RunnableFactory factory = new WriteFTGSRunner(testSession, intFields, stringFields, 8);

            simulator = new ClusterSimulator(8, nMetrics);

            final Thread t = simulator.simulateServer(factory);
            final RawFTGSIterator ftgsIterator = simulator.getIterator();

            compareIterators(ftgsIterator, verificationIter, nMetrics);

            t.join();
        } finally {
            if(simulator != null) {
                simulator.close();
            }
        }
    }

    @Test
    public void testNativeMetricInverter() throws IOException, FlamdexOutOfMemoryException {
        final long seed = rand.nextLong();
//        final long seed = 5934453071102157952L;
        rand.setSeed(seed);
        System.out.println("Random seed: " + seed);
        final int numDocs = (1 << 17) + rand.nextInt(1 << 17)+ 1;  // 128K

        final int nMetrics = MetricMaxSizes.values().length;
        final String[] metricNames = new String[nMetrics];
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
        for (final MetricMaxSizes sz : MetricMaxSizes.values()) {
            final FieldDesc fd = new FieldDesc();
            fd.isIntfield = true;
            fd.name = metricNames[j++];
            fd.termGenerator = sz;
            fd.numDocs = numDocs;
            fieldDescs.add(fd);
        }

        final Path shardName = generateShard(fieldDescs);
        System.out.println(shardName);
        final Path shardCopy = copyShard(shardName);

        final SimpleFlamdexReader verificationShard = SimpleFlamdexReader.open(shardCopy);
        final SimpleFlamdexReader testShard = SimpleFlamdexReader.open(shardName);
        final int nDocs = testShard.getNumDocs();

        // jit the code
        long foo = 0;
        final int[] d = new int[1];
        final long[] v = new long[1];
        d[0] = nDocs - 3;
        for (int i = 0; i < 100; i++) {
            for (final String metric : metricNames) {
                final IntValueLookup verificationIVL = verificationShard.getMetricJava(metric);
                verificationIVL.lookup(d, v, 1);
                foo += v[0];
                final IntValueLookup testIVL = testShard.getMetric(metric);
                testIVL.lookup(d, v, 1);
                foo += v[0];
            }
        }
        System.out.println("foo: " + foo);

        for (final String metric : metricNames) {
            final long t1 = System.nanoTime();
            final IntValueLookup verificationIVL = verificationShard.getMetricJava(metric);
            final long t2 = System.nanoTime();
            final IntValueLookup testIVL = testShard.getMetric(metric);
            final long t3 = System.nanoTime();
            System.out.println("Timings (max " + testIVL.getMax() + ") - "
                                       + " java: " + (t2 - t1)
                                       + " native: " + (t3 - t2));

            int offset = 0;
            final int[] docIds = new int[1024];
            final long[] goodValues = new long[1024];
            final long[] testValues = new long[1024];
            do {
                final int count = (nDocs - offset > 1024) ? 1024 : nDocs - offset;
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

        System.gc();
    }

    private void compareIterators(final FTGSIterator test, final FTGSIterator valid, final int numStats) {
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
                final long validFreq = valid.termDocFreq();
                final long testFreq = test.termDocFreq();
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

    private void readAllIterator(final FTGSIterator iterator, final int numStats) {
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
