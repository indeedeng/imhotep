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
package com.indeed.flamdex.simple;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.indeed.ParameterizedUtils;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.TestFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jsgroth
 */
@RunWith(Parameterized.class)
public class SimpleFlamdexTest {
    private static final Logger log = Logger.getLogger(SimpleFlamdexTest.class);

    private final SimpleFlamdexReader.Config config;

    @Parameterized.Parameters
    public static Iterable<SimpleFlamdexReader.Config[]> configs() {
        return ParameterizedUtils.getFlamdexConfigs();
    }

    public SimpleFlamdexTest(final SimpleFlamdexReader.Config config) {
        this.config = config;
    }

    private static boolean btreeExists(final Path btreePath) throws IOException {
        return Files.exists(btreePath) && (Files.list(btreePath).count() > 0);
    }

    @Test
    public void testBTreesWrittenWhenNeeded() throws IOException {
        final Path dir = Files.createTempDirectory("flamdex-test");
        try {
            writeIndex(dir);

            final Path intBtreePath = dir.resolve("fld-f1.intindex64");
            final Path strBtreePath = dir.resolve("fld-f2.strindex");

            assertTrue(btreeExists(intBtreePath));
            assertTrue(btreeExists(strBtreePath));

            TestFileUtils.deleteDirTree(intBtreePath);
            TestFileUtils.deleteDirTree(strBtreePath);

            assertFalse(btreeExists(intBtreePath));
            assertFalse(btreeExists(strBtreePath));

            config.setWriteBTreesIfNotExisting(false);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir, config);
            r.close();

            assertFalse(btreeExists(intBtreePath));
            assertFalse(btreeExists(strBtreePath));

            config.setWriteBTreesIfNotExisting(true);
            final SimpleFlamdexReader r2 = SimpleFlamdexReader.open(dir, config);
            r2.close();

            assertTrue(btreeExists(intBtreePath));
            assertTrue(btreeExists(strBtreePath));
        } finally {
            TestFileUtils.deleteDirTree(dir);
        }
    }

    @Test
    public void testEmptyFields() throws IOException {
        final Path dir = Files.createTempDirectory("flamdex-test");
        try {
            final SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, 5L, true);
            w.getIntFieldWriter("if1").close();
            w.getStringFieldWriter("sf1").close();
            w.close();

            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir, config);
            final IntTermIterator it = r.getIntTermIterator("if1");
            assertFalse(it.next());
            final StringTermIterator sit = r.getStringTermIterator("sf1");
            assertFalse(sit.next());
            it.close();
            sit.close();
            r.close();
        } finally {
            TestFileUtils.deleteDirTree(dir);
        }
    }
    private final Random rand = new Random();

    @Test
    public void testIt() throws IOException {
        final Path dir = Files.createTempDirectory("flamdex-test");
        try {
            writeAndRead(dir);
        } finally {
            TestFileUtils.deleteDirTree(dir);
        }
    }

    @Test
    public void testGetMetric() throws IOException, FlamdexOutOfMemoryException, ImhotepOutOfMemoryException {
        final Path dir = Files.createTempDirectory("flamdex-test");
        try {
            internalTestGetMetric(dir);
        } finally {
            TestFileUtils.deleteDirTree(dir);
        }
    }

    @Test
    public void testValidFieldName() throws IOException, FlamdexOutOfMemoryException {
        final Path dir = Files.createTempDirectory("flamdex-test");
        try {
            final SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, 5L, true);
            w.getIntFieldWriter("if1").close();
            w.getStringFieldWriter("sf1").close();

            try {
                w.getIntFieldWriter("[A-Za-z_][A-Za-z0-9_]* what's a string that doesn't match this david.");
                fail();
            } catch (final IllegalArgumentException e) {
                assertTrue(true);
            }
            try {
                w.getStringFieldWriter("[A-Za-z_][A-Za-z0-9_]* what's a string that doesn't match this david.");
                fail();
            } catch (final IllegalArgumentException e) {
                assertTrue(true);
            }
            w.close();
        } finally {
            TestFileUtils.deleteDirTree(dir);
        }
    }

    private void internalTestGetMetric(final Path dir) throws IOException, FlamdexOutOfMemoryException {
        getMetricCase(dir, 2);
        getMetricCase(dir, 256);
        getMetricCase(dir, 65536);
        getMetricCase(dir, Integer.MAX_VALUE);
    }

    private void getMetricCase(final Path dir, final int maxTermVal) throws IOException, FlamdexOutOfMemoryException {
        for (int i = 0; i < 5; ++i) {
            final long[] cache = writeGetMetricIndex(dir, maxTermVal);
            final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir, config);
            // do it multiple times because these methods update internal state, make sure nothing unexpectedly weird happens
            for (int j = 0; j < 3; ++j) {
                final long memReq = r.memoryRequired("if1");
                final IntValueLookup ivl = r.getMetric("if1");
                assertEquals(memReq, ivl.memoryUsed());
                final int[] docIds = new int[r.getNumDocs()];
                final long[] values = new long[r.getNumDocs()];
                for (int doc = 0; doc < docIds.length; ++doc) {
                    docIds[doc] = doc;
                }
                ivl.lookup(docIds, values, r.getNumDocs());
                assertEquals(Longs.asList(cache), Longs.asList(values));
                ivl.close();
            }
            r.close();
            FileUtils.cleanDirectory(dir.toFile());
        }
    }

    private long[] writeGetMetricIndex(final Path dir, final int maxTermVal) throws IOException {
        final SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, 20000L, true);
        final IntFieldWriter ifw = w.getIntFieldWriter("if1");
        final List<Integer> docs = Lists.newArrayListWithCapacity(20000);
        for (int i = 0; i < 20000; i++) {
            docs.add(i);
        }
        final long[] cache = new long[20000];
        if (maxTermVal < docs.size()) {
            final int term = rand.nextInt(maxTermVal);
            ifw.nextTerm(term);
            for (final int doc : docs) {
                ifw.nextDoc(doc);
                cache[doc] = term;
            }
        } else {
            final Map<Integer, int[]> map = Maps.newTreeMap();
            while (!docs.isEmpty()) {
                final int term = rand.nextInt(maxTermVal);
                if (map.containsKey(term)) {
                    continue;
                }
                final int numDocs = docs.size() > 1 ? rand.nextInt(docs.size() - 1) + 1 : 1;
                final int[] selectedDocs = new int[numDocs];
                for (int i = 0; i < numDocs; ++i) {
                    selectedDocs[i] = docs.remove(rand.nextInt(docs.size()));
                }
                Arrays.sort(selectedDocs);
                map.put(term, selectedDocs);
            }
            for (final int term : map.keySet()) {
                ifw.nextTerm(term);
                final int[] selectedDocs = map.get(term);
                for (final int doc : selectedDocs) {
                    ifw.nextDoc(doc);
                    cache[doc] = term;
                }
            }
        }
        ifw.close();
        w.close();

        return cache;
    }

    public void writeAndRead(final Path dir) throws IOException {
        writeIndex(dir);

        readCase1(dir);

        readCase2(dir);

        readCase3(dir);
    }

    private void readCase3(final Path dir) throws IOException {
        final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir, config);
        final StringTermDocIterator it = r.getStringTermDocIterator("f2");
        final int[] docBuffer = new int[20];

        assertTrue(it.nextTerm());
        assertEquals(it.term(), "");
        assertEquals(it.termStringLength(), 0);
        assertEquals(it.docFreq(), 2);
        assertEquals(it.fillDocIdBuffer(docBuffer), 2);
        assertEquals(docBuffer[0], 2);
        assertEquals(docBuffer[1], 5);

        assertTrue(it.nextTerm());
        assertEquals(it.term(), "a");
        assertEquals(it.termStringLength(), 1);
        assertEquals(it.docFreq(), 2);
        assertEquals(it.fillDocIdBuffer(docBuffer), 2);
        assertEquals(docBuffer[0], 4);
        assertEquals(docBuffer[1], 7);

        assertTrue(it.nextTerm());
        assertEquals(it.term(), "ffffffffff");
        assertEquals(it.termStringLength(), 10);
        assertEquals(it.docFreq(), 3);
        assertEquals(it.fillDocIdBuffer(docBuffer), 3);
        assertEquals(docBuffer[0], 2);
        assertEquals(docBuffer[1], 5);
        assertEquals(docBuffer[2], 9);

        assertTrue(it.nextTerm());
        assertEquals(it.term(), "lollerskates");
        assertEquals(it.termStringLength(), 12);
        assertEquals(it.docFreq(), 2);
        assertEquals(it.fillDocIdBuffer(docBuffer), 2);
        assertEquals(docBuffer[0], 7);
        assertEquals(docBuffer[1], 8);

        assertFalse(it.nextTerm());

        it.close();
        r.close();
    }

    private void readCase2(final Path dir) throws IOException {
        final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir, config);
        final DocIdStream dis = r.getDocIdStream();
        final int[] docIdBuf = new int[2];
        final StringTermIterator strItr = r.getStringTermIterator("f2");

        strItr.reset("ffffffffff");
        assertTrue(strItr.next());
        assertEquals("ffffffffff", strItr.term());
        assertEquals(3, strItr.docFreq());
        dis.reset(strItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(2, docIdBuf[0]);
        assertEquals(5, docIdBuf[1]);
        assertEquals(1, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(9, docIdBuf[0]);

        assertTrue(strItr.next());
        assertEquals("lollerskates", strItr.term());
        assertEquals(2, strItr.docFreq());
        dis.reset(strItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(7, docIdBuf[0]);
        assertEquals(8, docIdBuf[1]);
        assertEquals(0, dis.fillDocIdBuffer(docIdBuf));

        assertFalse(strItr.next());

        strItr.reset("zzzzzzzzzzzzz");
        assertFalse(strItr.next());

        final IntTermIterator intItr = r.getIntTermIterator("f1");
        intItr.reset(9000);

        assertTrue(intItr.next());
        assertEquals(9000, intItr.term());
        assertEquals(4, intItr.docFreq());
        dis.reset(intItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(3, docIdBuf[0]);
        assertEquals(7, docIdBuf[1]);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(8, docIdBuf[0]);
        assertEquals(9, docIdBuf[1]);
        assertEquals(0, dis.fillDocIdBuffer(docIdBuf));

        assertFalse(intItr.next());

        intItr.reset(999999999);
        assertFalse(intItr.next());

        intItr.close();
        strItr.close();
        dis.close();
        r.close();
    }

    private void readCase1(final Path dir) throws IOException {
        final SimpleFlamdexReader reader = SimpleFlamdexReader.open(dir, config);

        assertEquals(1, reader.getIntFields().size());
        assertEquals("f1", reader.getIntFields().iterator().next());
        assertEquals(1, reader.getStringFields().size());
        assertEquals("f2", reader.getStringFields().iterator().next());

        assertEquals(10, reader.getNumDocs());

        final DocIdStream dis = reader.getDocIdStream();
        final int[] docIdBuf = new int[2];

        final SimpleIntTermIterator intItr = reader.getIntTermIterator("f1");

        assertTrue(intItr.next());
        assertEquals(2, intItr.term());
        assertEquals(3, intItr.docFreq());
        assertEquals(0L, intItr.getOffset());
        dis.reset(intItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(0, docIdBuf[0]);
        assertEquals(4, docIdBuf[1]);
        assertEquals(1, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(9, docIdBuf[0]);

        assertTrue(intItr.next());
        assertEquals(99, intItr.term());
        assertEquals(2, intItr.docFreq());
        dis.reset(intItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(5, docIdBuf[0]);
        assertEquals(6, docIdBuf[1]);
        assertEquals(0, dis.fillDocIdBuffer(docIdBuf));

        assertTrue(intItr.next());
        assertEquals(101, intItr.term());
        assertEquals(3, intItr.docFreq());
        dis.reset(intItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(0, docIdBuf[0]);
        assertEquals(1, docIdBuf[1]);
        assertEquals(1, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(2, docIdBuf[0]);

        assertTrue(intItr.next());
        assertEquals(9000, intItr.term());
        assertEquals(4, intItr.docFreq());
        dis.reset(intItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(3, docIdBuf[0]);
        assertEquals(7, docIdBuf[1]);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(8, docIdBuf[0]);
        assertEquals(9, docIdBuf[1]);
        assertEquals(0, dis.fillDocIdBuffer(docIdBuf));

        assertFalse(intItr.next());

        final SimpleStringTermIterator strItr = reader.getStringTermIterator("f2");

        assertTrue(strItr.next());
        assertEquals("", strItr.term());
        assertEquals(2, strItr.docFreq());
        dis.reset(strItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(2, docIdBuf[0]);
        assertEquals(5, docIdBuf[1]);
        assertEquals(0, dis.fillDocIdBuffer(docIdBuf));

        assertTrue(strItr.next());
        assertEquals("a", strItr.term());
        assertEquals(2, strItr.docFreq());
        dis.reset(strItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(4, docIdBuf[0]);
        assertEquals(7, docIdBuf[1]);
        assertEquals(0, dis.fillDocIdBuffer(docIdBuf));

        assertTrue(strItr.next());
        assertEquals("ffffffffff", strItr.term());
        assertEquals(3, strItr.docFreq());
        dis.reset(strItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(2, docIdBuf[0]);
        assertEquals(5, docIdBuf[1]);
        assertEquals(1, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(9, docIdBuf[0]);

        assertTrue(strItr.next());
        assertEquals("lollerskates", strItr.term());
        assertEquals(2, strItr.docFreq());
        dis.reset(strItr);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuf));
        assertEquals(7, docIdBuf[0]);
        assertEquals(8, docIdBuf[1]);
        assertEquals(0, dis.fillDocIdBuffer(docIdBuf));

        assertFalse(strItr.next());

        intItr.close();
        strItr.close();
        dis.close();
        reader.close();
    }

    private void writeIndex(final Path dir) throws IOException {
        final SimpleFlamdexWriter writer = new SimpleFlamdexWriter(dir, 10);
        final IntFieldWriter ifw = writer.getIntFieldWriter("f1");
        ifw.nextTerm(2);
        ifw.nextDoc(0);
        ifw.nextDoc(4);
        ifw.nextDoc(9);
        try {
            ifw.nextDoc(10); // doc >= numDocs
            fail();
        } catch (final IllegalArgumentException e) {
            // pass
        }

        try {
            ifw.nextDoc(5); // doc out of order
            fail();
        } catch (final IllegalArgumentException e) {
            // pass
        }

        ifw.nextTerm(3); // should not be written because docFreq == 0
        ifw.nextTerm(99);
        ifw.nextDoc(5);
        ifw.nextDoc(6);
        ifw.nextTerm(101);
        ifw.nextDoc(0);
        ifw.nextDoc(1);
        ifw.nextDoc(2);

        try {
            ifw.nextTerm(10); // term out of order
            fail();
        } catch (final IllegalArgumentException e) {
            // pass
        }

        ifw.nextTerm(9000);
        ifw.nextDoc(3);
        ifw.nextDoc(7);
        ifw.nextDoc(8);
        ifw.nextDoc(9);
        ifw.close();

        final StringFieldWriter sfw = writer.getStringFieldWriter("f2");
        sfw.nextTerm("");
        sfw.nextDoc(2);
        sfw.nextDoc(5);
        sfw.nextTerm("a");
        sfw.nextDoc(4);
        sfw.nextDoc(7);
        sfw.nextTerm("d"); // should not be written because docFreq == 0
        sfw.nextTerm("ffffffffff");
        sfw.nextDoc(2);
        sfw.nextDoc(5);
        sfw.nextDoc(9);

        try {
            sfw.nextTerm("eeeeeeeeeeeeeeeeeeeeeeeee"); // term out of order
            fail();
        } catch (final IllegalArgumentException e) {
            // pass
        }

        sfw.nextTerm("lollerskates");
        sfw.nextDoc(7);
        sfw.nextDoc(8);
        sfw.close();

        writer.close();
    }

    @Test
    public void testTermDocIterators() throws IOException {
        final Path dir = Files.createTempDirectory("flamdex-test");
        try {
            writeIndex(dir);
            try(
                final SimpleFlamdexReader reader = SimpleFlamdexReader.open(dir, config)) {
                final int[] docIds = new int[1];

                // iterating field and first document from field
                try (final IntTermDocIterator intIter = reader.getIntTermDocIterator("f1")) {
                    assertTrue(intIter.nextTerm());
                    assertEquals(2, intIter.term());
                    assertEquals(1, intIter.fillDocIdBuffer(docIds));
                    assertEquals(0, docIds[0]);
                    assertTrue(intIter.nextTerm());
                    assertEquals(99, intIter.term());
                    assertEquals(1, intIter.fillDocIdBuffer(docIds));
                    assertEquals(5, docIds[0]);
                    assertTrue(intIter.nextTerm());
                    assertEquals(101, intIter.term());
                    assertEquals(1, intIter.fillDocIdBuffer(docIds));
                    assertEquals(0, docIds[0]);
                    assertTrue(intIter.nextTerm());
                    assertEquals(9000, intIter.term());
                    assertEquals(1, intIter.fillDocIdBuffer(docIds));
                    assertEquals(3, docIds[0]);
                    assertFalse(intIter.nextTerm());
                }

                try (final StringTermDocIterator strIter = reader.getStringTermDocIterator("f2")) {
                    assertTrue(strIter.nextTerm());
                    assertEquals("", strIter.term());
                    assertEquals(1, strIter.fillDocIdBuffer(docIds));
                    assertEquals(2, docIds[0]);
                    assertTrue(strIter.nextTerm());
                    assertEquals("a", strIter.term());
                    assertEquals(1, strIter.fillDocIdBuffer(docIds));
                    assertEquals(4, docIds[0]);
                    assertTrue(strIter.nextTerm());
                    assertEquals("ffffffffff", strIter.term());
                    assertEquals(1, strIter.fillDocIdBuffer(docIds));
                    assertEquals(2, docIds[0]);
                    assertTrue(strIter.nextTerm());
                    assertEquals("lollerskates", strIter.term());
                    assertEquals(1, strIter.fillDocIdBuffer(docIds));
                    assertEquals(7, docIds[0]);
                    assertFalse(strIter.nextTerm());
                }
            }
        } finally {
            TestFileUtils.deleteDirTree(dir);
        }
    }
}
