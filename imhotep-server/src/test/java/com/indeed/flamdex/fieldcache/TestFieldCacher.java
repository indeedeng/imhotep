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
 package com.indeed.flamdex.fieldcache;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.AbstractFlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.io.TestFileUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestFieldCacher {
    private final Random rand = new Random();

    @Test
    public void testFieldCacher() throws IOException {
        runCacheTest(Integer.MAX_VALUE, 65535, FieldCacher.INT, 40);
        runCacheTest(65535, 255, FieldCacher.CHAR, 20);
        runCacheTest(255, 1, FieldCacher.BYTE, 10);
        runCacheTest(1, 0, FieldCacher.BITSET, 8);
    }

    private void runCacheTest(final int maxVal,
                              final int lowerMaxVal,
                              final FieldCacher expectedType,
                              final long expectedMemory) throws IOException {
        for (int i = 0; i < 10; ++i) {
            final MockFlamdexReader r = new MockFlamdexReader(
                    Collections.singletonList("f"),
                    Collections.<String>emptyList(),
                    Collections.singletonList("f"),
                    10);
            final long maxTerm = rand.nextInt(maxVal - lowerMaxVal) + lowerMaxVal + 1;
            final int maxTermDoc = rand.nextInt(10);
            r.addIntTerm("f", maxTerm, maxTermDoc);
            final List<Integer> docs = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            docs.remove(Integer.valueOf(maxTermDoc));
            final Set<Integer> terms = new HashSet<>();
            final long[] cache = new long[10];
            cache[maxTermDoc] = maxTerm;
            if (lowerMaxVal >= docs.size()) {
                while (!docs.isEmpty()) {
                    final int numDocs = docs.size() > 1 ? rand.nextInt(docs.size() - 1) + 1 : 1;
                    final int term = rand.nextInt(lowerMaxVal + 1);
                    if (terms.contains(term)) {
                        continue;
                    }
                    final List<Integer> termDocs = new ArrayList<>();
                    for (int j = 0; j < numDocs; ++j) {
                        termDocs.add(docs.remove(rand.nextInt(docs.size())));
                    }
                    Collections.sort(termDocs);
                    r.addIntTerm("f", term, termDocs);
                    terms.add(term);
                    for (final int doc : termDocs) {
                        cache[doc] = term;
                    }
                }
            } else {
                final int term = rand.nextInt(lowerMaxVal + 1);
                r.addIntTerm("f", term, docs);
                for (final int doc : docs) {
                    cache[doc] = term;
                }
            }

            final AbstractFlamdexReader.MinMax minMax = new AbstractFlamdexReader.MinMax();
            final FieldCacher fieldCacher = FieldCacherUtil.getCacherForField("f", r, minMax);
            assertEquals(expectedType, fieldCacher);
            assertEquals(expectedMemory, fieldCacher.memoryRequired(r.getNumDocs()));
            assertEquals(maxTerm, minMax.max);
            final IntValueLookup ivl = fieldCacher.newFieldCache("f", r, minMax.min, minMax.max);
            verifyCache(cache, ivl);

            final Path tempDir = Files.createTempDirectory("asdf");
            try {
                for (int x = 0; x < 5; ++x) {
                    if (x > 0) {
                        assertTrue(Files.exists(tempDir.resolve(fieldCacher.getMMapFileName("f"))));
                    } else {
                        assertFalse(Files.exists(tempDir.resolve(fieldCacher.getMMapFileName("f"))));
                    }
                    final IntValueLookup mmivl = fieldCacher.newMMapFieldCache("f",
                                                                         r,
                                                                         tempDir,
                                                                         minMax.min,
                                                                         minMax.max);
                    verifyCache(cache, mmivl);
                    assertTrue(Files.exists(tempDir.resolve(fieldCacher.getMMapFileName("f"))));
                    mmivl.close();
                    assertTrue(Files.exists(tempDir.resolve(fieldCacher.getMMapFileName("f"))));
                }
            }
            finally {
                TestFileUtils.deleteDirTree(tempDir);
            }
        }
    }

    private static void verifyCache(final long[] cache, final IntValueLookup ivl) {
        final int[] docIds = new int[10];
        for (int j = 0; j < 10; ++j) {
            docIds[j] = j;
        }
        final long[] values = new long[10];
        ivl.lookup(docIds, values, 10);
        assertEquals(Longs.asList(cache), Longs.asList(values));
    }
}
