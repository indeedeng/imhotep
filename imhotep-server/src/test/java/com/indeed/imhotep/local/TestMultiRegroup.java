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

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.indeed.ParameterizedUtils;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.TestFileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author jfinley
 */
@RunWith(Parameterized.class)
public class TestMultiRegroup {

    private final SimpleFlamdexReader.Config config;

    @Parameterized.Parameters
    public static Iterable<SimpleFlamdexReader.Config[]> configs() {
        return ParameterizedUtils.getFlamdexConfigs();
    }

    public TestMultiRegroup(final SimpleFlamdexReader.Config config) {
        this.config = config;
    }

    private static final int N_DOCS = 4096;
    private static final int N_FIELDS = 7;
    private static final int MAX_N_TERMS = 1024;

    private Path shardDir;
    private Random random;
    private List<String> fields;

    private void setUpFields() {
        fields = new ArrayList<>();
        for (int n = 0; n < N_FIELDS; ++n) {
            fields.add("if" + n);
        }
    }

    @Before
    public void setUp() throws IOException {

        MTImhotepLocalMultiSession.loadNativeLibrary();

        random = new Random(42);

        shardDir = TestFileUtils.createTempShard();

        try (final SimpleFlamdexWriter shardWriter = new SimpleFlamdexWriter(shardDir, N_DOCS)) {
            setUpFields();
            for (final String field : fields) {
                final IntFieldWriter ifw = shardWriter.getIntFieldWriter(field);
                final int nTerms = random.nextInt(MAX_N_TERMS);
                final long[] terms = generateTerms(nTerms);
                final SortedSetMultimap<Long, Integer> termsToDocIds = generateTermsToDocIds(terms);
                for (final long term : terms) {
                    ifw.nextTerm(term);
                    final SortedSet<Integer> docIds = termsToDocIds.get(term);
                    for (final Integer docId : docIds) {
                        ifw.nextDoc(docId);
                    }
                }
                ifw.close();
            }
        }
    }

    private SortedSetMultimap<Long, Integer> generateTermsToDocIds(final long[] terms) {
        final SortedSetMultimap<Long, Integer> result = TreeMultimap.create();
        for (int nId = 0; nId < N_DOCS; ++nId) {
            final long term = terms[random.nextInt(terms.length)];
            result.put(term, nId);
        }
        return result;
    }

    private long[] generateTerms(final int count) {
        final long[] result = new long[count];
        for (int nTerm = 0; nTerm < result.length; ++nTerm) {
            result[nTerm] = nTerm + 1;
        }
        Arrays.sort(result);
        return result;
    }

    private int[] normalRegroup(final GroupMultiRemapRule[] regroupRule)
            throws IOException, ImhotepOutOfMemoryException {
        try (final SimpleFlamdexReader reader = SimpleFlamdexReader.open(shardDir, config);
             final ImhotepLocalSession session = new ImhotepJavaLocalSession(reader)) {
            final int[] result = new int[N_DOCS];
            session.regroup(regroupRule);
            session.exportDocIdToGroupId(result);
            return result;
        }
    }

    private int[] nativeRegroup(final GroupMultiRemapRule[] regroupRule)
            throws IOException, ImhotepOutOfMemoryException {
        try (final SimpleFlamdexReader reader = SimpleFlamdexReader.open(shardDir, config);
             final ImhotepNativeLocalSession session =
                     new ImhotepNativeLocalSession(reader)) {
            final int[] result = new int[N_DOCS];
            final MultiCacheConfig config = new MultiCacheConfig();
            config.calcOrdering(new StatLookup[]{session.statLookup}, session.numStats);
            session.buildMultiCache(config);

            final AtomicLong theAtomicPunk = new AtomicLong(100000);
            final ImhotepLocalSession[] localSessions = new ImhotepLocalSession[]{session};

            try( final ImhotepMemoryPool memoryPool = new ImhotepMemoryPool(Long.MAX_VALUE);
                 final MTImhotepLocalMultiSession mtSession =
                    new MTImhotepLocalMultiSession(localSessions,
                            new MemoryReservationContext(memoryPool),
                            theAtomicPunk) ) {
                mtSession.regroup(regroupRule);
                session.exportDocIdToGroupId(result);
            }
            return result;
        }
    }

    @Test
    public void testMultiCacheIntRegroup()
            throws IOException, ImhotepOutOfMemoryException {
        final RegroupCondition[] conditions = new RegroupCondition[10];
        final int[] positiveGroups = new int[10];
        for (int i = 1; i <= 10; i++) {
            conditions[i - 1] = new RegroupCondition("if1", true, i, null, false);
            positiveGroups[i - 1] = i;
        }
        final GroupMultiRemapRule[] regroupRule =
                new GroupMultiRemapRule[]{
                        new GroupMultiRemapRule(1, 0, positiveGroups, conditions)
                };
        final int[] normalRegroup = normalRegroup(regroupRule);
        final int[] nativeRegroup = nativeRegroup(regroupRule);
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
