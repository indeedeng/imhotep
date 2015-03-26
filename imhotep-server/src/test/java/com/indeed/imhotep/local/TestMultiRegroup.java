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

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import com.indeed.flamdex.api.*;
import com.indeed.flamdex.simple.*;
import com.indeed.flamdex.writer.*;
import com.indeed.imhotep.api.*;
import com.indeed.imhotep.*;
import com.indeed.util.io.Files;

import java.io.*;
import java.util.*;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * @author jfinley
 */
public class TestMultiRegroup extends TestCase {

    private static final int N_DOCS      = 4096;
    private static final int N_FIELDS    = 7;
    private static final int MAX_N_TERMS = 1024;

    File           shardDir;
    Random         random;
    ArrayList<String> fields;

    private void setUpFields() {
        fields = new ArrayList<String>();
        for (int n = 0; n < N_FIELDS; ++n) {
            fields.add("if" + n);
        }
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        MTImhotepLocalMultiSession.loadNativeLibrary();
        //  System.load("/home/johnf/work/imhotep/imhotep-server/src/main/native/optimized_ftgs/optimized/libftgs.so.1.0.1");

        random = new Random(42);

        final String shardName = Files.getTempDirectory(this.getClass().getSimpleName(), "");
        shardDir = new File(shardName);

        try (final SimpleFlamdexWriter shardWriter = new SimpleFlamdexWriter(shardName, N_DOCS)) {
                setUpFields();
                for (final String field : fields) {
                    final IntFieldWriter ifw = shardWriter.getIntFieldWriter(field);
                    final int nTerms = random.nextInt(MAX_N_TERMS);
                    final long[] terms = generateTerms(nTerms);
                    SortedSetMultimap<Long, Integer> termsToDocIds = generateTermsToDocIds(terms);
                    for (long term: terms) {
                        ifw.nextTerm(term);
                        SortedSet<Integer> docIds = termsToDocIds.get(Long.valueOf(term));
                        for (Integer docId: docIds) {
                            ifw.nextDoc(docId.intValue());
                        }
                    }
                    ifw.close();
                }
            }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        Files.deleteOrDie(shardDir.toString());
    }

    private SortedSetMultimap<Long, Integer> generateTermsToDocIds(final long[] terms) {
        SortedSetMultimap<Long, Integer> result = TreeMultimap.create();
        for (int nId = 0; nId < N_DOCS; ++nId) {
            final long term = terms[random.nextInt(terms.length)];
            result.put(Long.valueOf(term), Integer.valueOf(nId));
        }
        return result;
    }

    private long[] generateTerms(final int count) {
        long[] result = new long[count];
        for (int nTerm = 0; nTerm < result.length; ++nTerm) {
            result[nTerm] = random.nextLong();
        }
        Arrays.sort(result);
        return result;
    }

    @Test
    public void testRandomMultiIntRegroup()
        throws IOException, ImhotepOutOfMemoryException {
        try (final SimpleFlamdexReader reader = SimpleFlamdexReader.open(shardDir.toString());
             final ImhotepLocalSession session = new ImhotepLocalSession(reader)) {
                final double[] percentages = new double[] { 0.10, 0.50 };
                final int[] resultGroups = new int[] { 5, 6, 7 };
                session.randomMultiRegroup(fields.get(0), true, "salt", 1,
                                           percentages, resultGroups);
            }
    }


    @Test
    public void testMultiCacheIntRegroup()
        throws IOException, ImhotepOutOfMemoryException {
        try (final SimpleFlamdexReader reader = SimpleFlamdexReader.open(shardDir.toString());
             final ImhotepLocalSession session = new ImhotepLocalSession(reader)) {
                final FlamdexReader[]  readers = new FlamdexReader[] { reader };
                final MultiCacheConfig  config = new MultiCacheConfig(1);
                final MultiCache[]      caches = new MultiCache[] { session.buildMulticache(config, 0) };

                final RegroupCondition[] conditions = new RegroupCondition[10];
                final int[] positiveGroups = new int[10];
                for (int i = 1; i <= 10; i++) {
                    conditions[i - 1] = new RegroupCondition("if1", true, i, null, false);
                    positiveGroups[i - 1] = i;
                }
                GroupMultiRemapRule[] gmrr = new GroupMultiRemapRule[] {
                    new GroupMultiRemapRule(1, 0, positiveGroups, conditions)
                };
                session.regroup(gmrr);
            }
    }

    @Test
    public void testSingleMultisplitIntRegroup()
        throws IOException, ImhotepOutOfMemoryException {
        try (final SimpleFlamdexReader reader = SimpleFlamdexReader.open(shardDir.toString());
             final ImhotepLocalSession session = new ImhotepLocalSession(reader)) {

                final RegroupCondition[] conditions = new RegroupCondition[10];
                final int[] positiveGroups = new int[10];
                for (int i = 1; i <= 10; i++) {
                    conditions[i - 1] = new RegroupCondition("if1", true, i, null, false);
                    positiveGroups[i - 1] = i;
                }
                session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(1, 0, positiveGroups,
                                                                                    conditions) });
                /*
                int[] docIdToGroup = new int[11];
                session.exportDocIdToGroupId(docIdToGroup);
                for (int docId = 0; docId < 10; docId++) {
                    final int actualGroup = docIdToGroup[docId];
                    final int expectedGroup = docId + 1;
                    assertEquals("doc id #" + docId + " was misgrouped;", expectedGroup, actualGroup);
                }
                assertEquals("doc id #10 should be in no group", 0, docIdToGroup[10]);
                */
            }
    }
}
