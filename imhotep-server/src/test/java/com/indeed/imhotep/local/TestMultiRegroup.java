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
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import junit.framework.TestCase;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

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
            result[nTerm] = nTerm + 1;
        }
        Arrays.sort(result);
        return result;
    }

    private int[] normalRegroup(GroupMultiRemapRule[] regroupRule)
        throws IOException, ImhotepOutOfMemoryException {
        try (final SimpleFlamdexReader reader = SimpleFlamdexReader.open(shardDir.toString());
             final ImhotepLocalSession session = new ImhotepJavaLocalSession(reader)) {
                int[] result = new int[N_DOCS];
                final FlamdexReader[]  readers = new FlamdexReader[] { reader };
                session.regroup(regroupRule);
                session.exportDocIdToGroupId(result);
                return result;
            }
    }

    private int[] nativeRegroup(GroupMultiRemapRule[] regroupRule)
        throws IOException, ImhotepOutOfMemoryException {
        try (final SimpleFlamdexReader reader = SimpleFlamdexReader.open(shardDir.toString());
             final ImhotepNativeLocalSession session=
             new ImhotepNativeLocalSession(reader)) {
                int[] result                   = new int[N_DOCS];
                final FlamdexReader[]  readers = new FlamdexReader[] { reader };
                final MultiCacheConfig config  = new MultiCacheConfig();
                config.calcOrdering(new StatLookup[] {session.statLookup}, session.numStats);
                session.buildMultiCache(config);

                final ExecutorService executor = Executors.newCachedThreadPool();
                AtomicLong theAtomicPunk = new AtomicLong(100000);
                ImhotepLocalSession[] localSessions = new ImhotepLocalSession[] { session };
                ImhotepMemoryPool memoryPool = new ImhotepMemoryPool(Long.MAX_VALUE);
                final MTImhotepLocalMultiSession mtSession =
                    new MTImhotepLocalMultiSession(localSessions,
                                                   new MemoryReservationContext(memoryPool),
                                                   executor, theAtomicPunk, true);
                mtSession.regroup(regroupRule);
                session.exportDocIdToGroupId(result);
                return result;
            }
    }

    public void testMultiCacheIntRegroup()
        throws IOException, ImhotepOutOfMemoryException {
        final RegroupCondition[] conditions = new RegroupCondition[10];
        final int[] positiveGroups = new int[10];
        for (int i = 1; i <= 10; i++) {
            conditions[i - 1] = new RegroupCondition("if1", true, i, null, false);
            positiveGroups[i - 1] = i;
        }
        GroupMultiRemapRule[] regroupRule =
            new GroupMultiRemapRule[] {
            new GroupMultiRemapRule(1, 0, positiveGroups, conditions)
        };
        int[] normalRegroup = normalRegroup(regroupRule);
        int[] nativeRegroup = nativeRegroup(regroupRule);
        for (int x = 0; x < nativeRegroup.length; ++x) {
            if (nativeRegroup[x] != normalRegroup[x]) {
                System.err.println("x: " + x +
                                   " native: " + nativeRegroup[x] +
                                   " normal: " + normalRegroup[x]);
            }
        }
        assertArrayEquals(normalRegroup, nativeRegroup);
    }
}
