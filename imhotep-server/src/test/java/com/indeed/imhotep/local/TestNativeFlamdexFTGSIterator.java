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

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.fieldcache.LongArrayIntValueLookup;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.io.TestFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author darren
 */
public class TestNativeFlamdexFTGSIterator {

    public static class FieldDesc {
        public String name;
        public boolean isIntfield;
        public int numDocs;
        public MetricMaxSizes termGenerator;
        public List<String> terms;
    }

    private List<String> stringTerms;
    private final Random rand = new Random();
    private static final int MAX_STRING_TERM_LEN = 128;

    @Before
    public void setUp() {
        final int numTerms = (1 << 12);  // 4K
        stringTerms = new ArrayList<>(numTerms);

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
        try (final IntFieldWriter ifw = w.getIntFieldWriter(intFieldName)) {

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
        }
    }

    private void createFlamdexStringField(final String stringFieldName,
                                          final int size,
                                          final SimpleFlamdexWriter w,
                                          final List<String> termsList,
                                          final long maxDocs) throws IOException {
        final int maxTermDocs = (size < 2000) ? size : size / 1000;
        final int maxUnassignedDocs = (size < 100) ? 10 : 100;
        try (final StringFieldWriter sfw = w.getStringFieldWriter(stringFieldName)) {

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
        }
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
        final Path dir = TestFileUtils.createTempShard(null, shardDate, ".native-ftgs-test.test-dir.");
        // find max # of docs
        long nDocs = 0;
        for (final FieldDesc fd : fieldDescs) {
            nDocs = (fd.numDocs > nDocs) ? fd.numDocs : nDocs;
        }

//        System.out.println("numdocs: " + nDocs);

        try (final SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, nDocs, true)) {

            for (final FieldDesc fd : fieldDescs) {
                if (fd.isIntfield) {
                    createFlamdexIntField(fd.name, fd.termGenerator, fd.numDocs, w, nDocs);
                } else {
                    createFlamdexStringField(fd.name, fd.numDocs, w, fd.terms, nDocs);
                }
            }
        }
        return dir;
    }

    private Path copyShard(final Path dir) throws IOException {

        final String srcShardName = dir.getFileName().toString();
        final DateTime shardTime = ShardTimeUtils.parseStart(srcShardName);
        final Path newDir = TestFileUtils.createTempShard(null, shardTime, ".native-ftgs-test.verify-dir.");

        FileUtils.copyDirectory(dir.toFile(), newDir.toFile());

        return newDir;
    }

    private String randomString(final int len) {
        return RandomStringUtils.random(len, 0, 0, true, false, null, rand);
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
                final IntValueLookup verificationIVL =
                        new LongArrayIntValueLookup(FlamdexUtils.cacheLongField(metric, verificationShard));
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
            final IntValueLookup verificationIVL =
                    new LongArrayIntValueLookup(FlamdexUtils.cacheLongField(metric, verificationShard));
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
}
