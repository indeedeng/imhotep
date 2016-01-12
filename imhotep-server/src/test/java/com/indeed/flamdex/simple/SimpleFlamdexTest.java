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
 package com.indeed.flamdex.simple;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.indeed.util.io.Files;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author jsgroth
 */
public class SimpleFlamdexTest extends TestCase {
    private final Random rand = new Random();

    @Test
    public void testEmptyFields() throws IOException {
        final String dir = Files.getTempDirectory("flamdex-test", "foo");
        try {
            SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, 5L, true);
            w.getIntFieldWriter("if1").close();
            w.getStringFieldWriter("sf1").close();
            w.close();

            SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            IntTermIterator it = r.getIntTermIterator("if1");
            assertFalse(it.next());
            StringTermIterator sit = r.getStringTermIterator("sf1");
            assertFalse(sit.next());
            it.close();
            sit.close();
            r.close();
        } finally {
            Files.delete(dir);
        }
    }

    @Test
    public void testIt() throws IOException {
        final String dir = Files.getTempDirectory("flamdex-test", "foo");
        try {
            writeAndRead(dir);
        } finally {
            Files.delete(dir);
        }
    }

    @Test
    public void testGetMetric() throws IOException, FlamdexOutOfMemoryException {
        final String dir = Files.getTempDirectory("flamdex-test", "foo");
        try {
            internalTestGetMetric(dir);
        } finally {
            Files.delete(dir);
        }
    }

    @Test
    public void testValidFieldName() throws IOException, FlamdexOutOfMemoryException {
        final String dir = Files.getTempDirectory("flamdex-test", "foo");
        try {
            SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, 5L, true);
            w.getIntFieldWriter("if1").close();
            w.getStringFieldWriter("sf1").close();

            try {
                w.getIntFieldWriter("[A-Za-z_][A-Za-z0-9_]* what's a string that doesn't match this david.");
                assertFalse(true);
            } catch (final IllegalArgumentException e) {
                assertTrue(true);
            }
            try {
                w.getStringFieldWriter("[A-Za-z_][A-Za-z0-9_]* what's a string that doesn't match this david.");
                assertFalse(true);
            } catch (final IllegalArgumentException e) {
                assertTrue(true);
            }
            w.close();
        } finally {
            Files.delete(dir);
        }
    }

    private void internalTestGetMetric(String dir) throws IOException, FlamdexOutOfMemoryException {
        getMetricCase(dir, 2);
        getMetricCase(dir, 256);
        getMetricCase(dir, 65536);
        getMetricCase(dir, Integer.MAX_VALUE);
    }

    private void getMetricCase(String dir, int maxTermVal) throws IOException, FlamdexOutOfMemoryException {
        for (int i = 0; i < 10; ++i) {
            long[] cache = writeGetMetricIndex(dir, maxTermVal);
            SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
            // do it multiple times because these methods update internal state, make sure nothing unexpectedly weird happens
            for (int j = 0; j < 3; ++j) {
                long memReq = r.memoryRequired("if1");
                IntValueLookup ivl = r.getMetric("if1");
                assertEquals(memReq, ivl.memoryUsed());
                int[] docIds = new int[r.getNumDocs()];
                long[] values = new long[r.getNumDocs()];
                for (int doc = 0; doc < docIds.length; ++doc) docIds[doc] = doc;
                ivl.lookup(docIds, values, r.getNumDocs());
                assertEquals(Longs.asList(cache), Longs.asList(values));
            }
        }
    }

    private long[] writeGetMetricIndex(String dir, int maxTermVal) throws IOException {
        SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, 10L, true);
        IntFieldWriter ifw = w.getIntFieldWriter("if1");
        List<Integer> docs = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        long[] cache = new long[10];
        if (maxTermVal < docs.size()) {
            int term = rand.nextInt(maxTermVal);
            ifw.nextTerm(term);
            for (int doc : docs) {
                ifw.nextDoc(doc);
                cache[doc] = term;
            }
        } else {
            Map<Integer, List<Integer>> map = Maps.newTreeMap();
            while (!docs.isEmpty()) {
                int term = rand.nextInt(maxTermVal);
                if (map.containsKey(term)) continue;
                int numDocs = docs.size() > 1 ? rand.nextInt(docs.size() - 1) + 1 : 1;
                List<Integer> selectedDocs = Lists.newArrayList();
                for (int i = 0; i < numDocs; ++i) {
                    selectedDocs.add(docs.remove(rand.nextInt(docs.size())));
                }
                Collections.sort(selectedDocs);
                map.put(term, selectedDocs);
            }
            for (int term : map.keySet()) {
                ifw.nextTerm(term);
                List<Integer> selectedDocs = map.get(term);
                for (int doc : selectedDocs) {
                    ifw.nextDoc(doc);
                    cache[doc] = term;
                }
            }
        }
        ifw.close();
        w.close();

        return cache;
    }

    public void writeAndRead(String dir) throws IOException {
        writeIndex(dir);

        readCase1(dir);

        readCase2(dir);

        readCase3(dir);
    }

    private void readCase3(String dir) throws IOException {
        final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
        final RawStringTermDocIterator it = r.getStringTermDocIterator("f2");
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
    }

    private void readCase2(String dir) throws IOException {
        final SimpleFlamdexReader r = SimpleFlamdexReader.open(dir);
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
    }

    private void readCase1(String dir) throws IOException {
        final SimpleFlamdexReader reader = SimpleFlamdexReader.open(dir);

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
    }

    private void writeIndex(String dir) throws IOException {
        final SimpleFlamdexWriter writer = new SimpleFlamdexWriter(dir, 10);
        final IntFieldWriter ifw = writer.getIntFieldWriter("f1");
        ifw.nextTerm(2);
        ifw.nextDoc(0);
        ifw.nextDoc(4);
        ifw.nextDoc(9);
        try {
            ifw.nextDoc(10); // doc >= numDocs
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            ifw.nextDoc(5); // doc out of order
            assertTrue(false);
        } catch (IllegalArgumentException e) {
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
            assertTrue(false);
        } catch (IllegalArgumentException e) {
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
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            // pass
        }

        sfw.nextTerm("lollerskates");
        sfw.nextDoc(7);
        sfw.nextDoc(8);
        sfw.close();

        writer.close();
    }
}
