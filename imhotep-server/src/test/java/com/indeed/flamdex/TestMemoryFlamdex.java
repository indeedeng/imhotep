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
 package com.indeed.flamdex;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author jsgroth
 */
public class TestMemoryFlamdex {

    File tmpDir;

    @Before
    public void setUp() throws Exception {
        tmpDir = File.createTempFile("tmp", "", new File("."));
        tmpDir.delete();
        tmpDir.mkdirs();
    }

    @After
    public void tearDown() throws Exception {
        PosixFileOperations.rmrf(tmpDir);
    }

    @Test
    public void testIntMaxValue() throws IOException {
        MemoryFlamdex fdx = new MemoryFlamdex().setNumDocs(1);
        IntFieldWriter ifw = fdx.getIntFieldWriter("if1");
        ifw.nextTerm(Integer.MAX_VALUE);
        ifw.nextDoc(0);
        ifw.close();
        fdx.close();

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        fdx.write(out);

        MemoryFlamdex fdx2 = new MemoryFlamdex();
        fdx2.readFields(ByteStreams.newDataInput(out.toByteArray()));

        innerTestIntMaxValue(fdx2);

        innerTestIntMaxValue(MemoryFlamdex.streamer(ByteStreams.newDataInput(out.toByteArray())));
    }

    private void innerTestIntMaxValue(FlamdexReader fdx2) {
        IntTermIterator iter = fdx2.getIntTermIterator("if1");
        DocIdStream dis = fdx2.getDocIdStream();
        int[] buf = new int[2];
        assertTrue(iter.next());
        assertEquals(Integer.MAX_VALUE, iter.term());
        dis.reset(iter);
        assertEquals(1, dis.fillDocIdBuffer(buf));
        assertEquals(0, buf[0]);
        assertFalse(iter.next());
    }

    @Test
    public void testStreamerDocIdStream() throws IOException {
        MemoryFlamdex fdx = new MemoryFlamdex();
        FlamdexDocument doc = new FlamdexDocument();
        doc.setIntField("if1", 5);
        for (int i = 0; i < 5; ++i) {
            fdx.addDocument(doc);
        }
        fdx.close();

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        fdx.write(out);

        FlamdexReader r = MemoryFlamdex.streamer(ByteStreams.newDataInput(out.toByteArray()));
        IntTermIterator iter = r.getIntTermIterator("if1");
        assertTrue(iter.next());
        assertEquals(5, iter.term());
        DocIdStream dis = r.getDocIdStream();
        dis.reset(iter);
        int[] buf = new int[2];
        assertEquals(2, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{0, 1}, buf);
        assertEquals(2, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{2, 3}, buf);
        assertEquals(1, dis.fillDocIdBuffer(buf));
        assertEquals(4, buf[0]);
    }

    @Test
    public void testDocWithRepeatingTerms() throws IOException {
        MemoryFlamdex fdx = new MemoryFlamdex();
        FlamdexDocument doc = new FlamdexDocument();
        doc.setIntField("if1", new long[]{1, 1, 1, 3, 3});
        fdx.addDocument(doc);
        doc.setIntField("if1", new long[]{1, 2, 2, 2, 4});
        fdx.addDocument(doc);
        fdx.close();

        innerTestBadDoc(fdx);

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        fdx.write(out);
        MemoryFlamdex fdx2 = new MemoryFlamdex();
        fdx2.readFields(ByteStreams.newDataInput(out.toByteArray()));

        innerTestBadDoc(fdx2);
    }

    private void innerTestBadDoc(MemoryFlamdex fdx) {
        assertEquals(2, fdx.getNumDocs());
        
        IntTermIterator iter = fdx.getIntTermIterator("if1");
        DocIdStream dis = fdx.getDocIdStream();
        int[] buf = new int[64];
        assertTrue(iter.next());
        assertEquals(1, iter.term());
        assertEquals(2, iter.docFreq());
        dis.reset(iter);
        assertEquals(2, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{0, 1}, Arrays.copyOf(buf, 2));
        assertTrue(iter.next());
        assertEquals(2, iter.term());
        assertEquals(1, iter.docFreq());
        dis.reset(iter);
        assertEquals(1, dis.fillDocIdBuffer(buf));
        assertEquals(1, buf[0]);
        assertTrue(iter.next());
        assertEquals(3, iter.term());
        assertEquals(1, iter.docFreq());
        dis.reset(iter);
        assertEquals(1, dis.fillDocIdBuffer(buf));
        assertEquals(0, buf[0]);
        assertTrue(iter.next());
        assertEquals(4, iter.term());
        assertEquals(1, iter.docFreq());
        dis.reset(iter);
        assertEquals(1, dis.fillDocIdBuffer(buf));
        assertEquals(1, buf[0]);
        assertFalse(iter.next());
        dis.close();
        iter.close();
    }

    @Test
    public void testTermWrite() throws IOException {
        MemoryFlamdex fdx = new MemoryFlamdex().setNumDocs(10);
        IntFieldWriter ifw = fdx.getIntFieldWriter("if1");
        ifw.nextTerm(5);
        ifw.nextDoc(0);
        ifw.nextDoc(3);
        ifw.nextDoc(4);
        ifw.nextDoc(5);
        ifw.nextTerm(6);
        ifw.nextTerm(99);
        ifw.nextDoc(1);
        ifw.nextDoc(2);
        ifw.nextDoc(7);
        ifw.close();
        StringFieldWriter sfw = fdx.getStringFieldWriter("sf1");
        sfw.nextTerm("a");
        sfw.nextDoc(2);
        sfw.nextDoc(8);
        sfw.nextDoc(9);
        sfw.nextTerm("b");
        sfw.nextDoc(1);
        sfw.nextDoc(8);
        sfw.nextTerm("c");
        sfw.nextTerm("d");
        sfw.nextDoc(5);
        sfw.nextDoc(6);
        sfw.close();
        fdx.close();

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        fdx.write(out);
        MemoryFlamdex fdx2 = new MemoryFlamdex();
        fdx2.readFields(ByteStreams.newDataInput(out.toByteArray()));

        verify(fdx2);
        verify(fdx);

        verify(fdx2.shallowCopy());
        verify(fdx.shallowCopy());
    }

    private void verify(FlamdexReader fdx) {
        assertEquals(10, fdx.getNumDocs());

        DocIdStream dis = fdx.getDocIdStream();
        IntTermIterator iti = fdx.getIntTermIterator("if1");
        assertTrue(iti.next());
        assertEquals(5, iti.term());
        assertEquals(4, iti.docFreq());
        int[] buf = new int[64];
        dis.reset(iti);
        assertEquals(4, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{0, 3, 4, 5}, Arrays.copyOf(buf, 4));
        assertTrue(iti.next());
        assertEquals(99, iti.term());
        assertEquals(3, iti.docFreq());
        dis.reset(iti);
        assertEquals(3, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{1, 2, 7}, Arrays.copyOf(buf, 3));
        assertFalse(iti.next());

        iti.reset(6);
        assertTrue(iti.next());
        assertEquals(99, iti.term());
        dis.reset(iti);
        assertEquals(3, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{1, 2, 7}, Arrays.copyOf(buf, 3));
        assertFalse(iti.next());

        iti.close();

        StringTermIterator sti = fdx.getStringTermIterator("sf1");
        assertTrue(sti.next());
        assertEquals("a", sti.term());
        assertEquals(3, sti.docFreq());
        dis.reset(sti);
        assertEquals(3, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{2, 8, 9}, Arrays.copyOf(buf, 3));
        assertTrue(sti.next());
        assertEquals("b", sti.term());
        assertEquals(2, sti.docFreq());
        dis.reset(sti);
        assertEquals(2, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{1, 8}, Arrays.copyOf(buf, 2));
        assertTrue(sti.next());
        assertEquals("d", sti.term());
        assertEquals(2, sti.docFreq());
        dis.reset(sti);
        assertEquals(2, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{5, 6}, Arrays.copyOf(buf, 2));
        assertFalse(sti.next());

        sti.reset("c");
        assertTrue(sti.next());
        assertEquals("d", sti.term());
        dis.reset(sti);
        assertEquals(2, dis.fillDocIdBuffer(buf));
        assertArrayEquals(new int[]{5, 6}, Arrays.copyOf(buf, 2));
        assertFalse(sti.next());

        sti.close();

        dis.close();
    }

    @Test
    public void testSerial() throws IOException {
        final MemoryFlamdex fdx = new MemoryFlamdex();
        final FlamdexDocument doc0 = new FlamdexDocument();
        doc0.setIntField("if1", 0);
        doc0.setIntField("if2", new long[]{5, 6, 7});
        doc0.setStringField("sf1", "asdf");
        fdx.addDocument(doc0);
        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.setIntField("if2", new long[]{3, 6});
        fdx.addDocument(doc1);
        final ByteArrayDataOutput out = ByteStreams.newDataOutput();
        fdx.write(out);
        final MemoryFlamdex fdx2 = new MemoryFlamdex();
        fdx2.readFields(ByteStreams.newDataInput(out.toByteArray()));
        assertEquals(2, fdx2.getNumDocs());
        fdx.readFields(ByteStreams.newDataInput(out.toByteArray()));
        assertEquals(2, fdx.getNumDocs());

        Map<String, List<FlamdexDocument>> ret = Maps.newHashMap();
        Random r = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 300; ++i) {
            Map<String, List<Long>> intFields = Maps.newHashMap();
            int numFields = r.nextInt(5) + 5;
            for (int j = 0; j < numFields; ++j) {
                int numTerms = r.nextInt(50) + 50;
                List<Long> terms = Lists.newArrayList();
                for (int k = 0; k < numTerms; ++k) {
                    terms.add(r.nextLong());
                }
                intFields.put("if" + j, terms);
            }
            Map<String, List<String>> stringFields = Maps.newHashMap();
            int numSFields = r.nextInt(5) + 5;
            for (int j = 0; j < numSFields; ++j) {
                int numTerms = r.nextInt(10) + 10;
                List<String> terms = Lists.newArrayList();
                for (int k = 0; k < numTerms; ++k) {
                    int termLen = r.nextInt(10) + 10;
                    sb.setLength(0);
                    for (int l = 0; l < termLen; ++l) {
                        sb.append((char)(r.nextInt(26) + 'a'));
                    }
                    terms.add(sb.toString());
                }
                stringFields.put("sf" + j, terms);
            }
            String key = "foo" + (r.nextInt(3) + 1);
            if (!ret.containsKey(key)) {
                ret.put(key, new ArrayList<FlamdexDocument>());
            }
            MockDoc doc = new MockDoc(Maps.transformValues(intFields, new Function<List<Long>, List<String>>() {
                @Nullable
                public List<String> apply(@Nullable final List<Long> input) {
                    return Lists.transform(input, new Function<Long, String>() {
                        @Nullable
                        public String apply(@Nullable final Long input) {
                            return String.valueOf(input);
                        }
                    });
                }
            }), stringFields);
            ret.get(key).add(doc.convert());
        }
        MemoryFlamdex original = new MemoryFlamdex();
        for (List<FlamdexDocument> docs : ret.values()) {
            for (FlamdexDocument doc : docs) {
                original.addDocument(doc);
            }
        }
        MemoryFlamdex copy = new MemoryFlamdex();
        ByteArrayDataOutput out2 = ByteStreams.newDataOutput();
        original.write(out2);
        copy.readFields(ByteStreams.newDataInput(out2.toByteArray()));

        assertTrue(FlamdexCompare.unorderedEquals(original, copy));
        int numDocs = 0;
        List<MemoryFlamdex> flamdexes = new ArrayList<MemoryFlamdex>();
        for (List<FlamdexDocument> docs : ret.values()) {
            MemoryFlamdex flamdex = new MemoryFlamdex();
            for (FlamdexDocument doc : docs) {
                flamdex.addDocument(doc);
            }
            numDocs += flamdex.getNumDocs();
            flamdexes.add(flamdex);
        }
        MemoryFlamdex merged = new MemoryFlamdex();
        merged.setNumDocs(numDocs);
        SimpleFlamdexWriter.merge(flamdexes, merged);
        assertTrue(FlamdexCompare.unorderedEquals(merged, original));
        File tmpFlamdexDir = new File(tmpDir, "tmpfdx");
        SimpleFlamdexWriter writer = new SimpleFlamdexWriter(tmpFlamdexDir.getPath(), numDocs, true);
        SimpleFlamdexWriter.writeFlamdex(merged, writer);
        writer.close();
        SimpleFlamdexReader reader = SimpleFlamdexReader.open(tmpFlamdexDir.getPath());
        assertTrue(FlamdexCompare.unorderedEquals(reader, original));
    }

    public class MockDoc {
        public Map<String, List<String>> intFields;
        public Map<String, List<String>> stringFields;

        public MockDoc() {}

        public MockDoc(Map<String, List<String>> intFields, Map<String, List<String>> stringFields) {
            this.intFields = intFields;
            this.stringFields = stringFields;
        }

        public FlamdexDocument convert() {
            FlamdexDocument doc = new FlamdexDocument();
            for (Map.Entry<String, List<String>> e : intFields.entrySet()) {
                doc.setIntField(e.getKey(), Lists.transform(e.getValue(), new Function<String, Long>() {
                    @Nullable
                    public Long apply(@Nullable final String input) {
                        return Long.parseLong(input);
                    }
                }));
            }
            for (Map.Entry<String, List<String>> e : stringFields.entrySet()) {
                doc.setStringField(e.getKey(), e.getValue());
            }
            return doc;
        }
    }

    @Test
    public void test2() throws IOException {
        final MemoryFlamdex fdx = new MemoryFlamdex();
        final FlamdexDocument doc0 = new FlamdexDocument();
        doc0.setIntField("clicked", 1);
        fdx.addDocument(doc0);
        fdx.close();

        final IntTermIterator it = fdx.getIntTermIterator("clicked");
        assertTrue(it.next());
        assertEquals(1, it.term());
        assertEquals(1, it.docFreq());
        assertFalse(it.next());

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        fdx.write(out);
        final MemoryFlamdex fdx2 = new MemoryFlamdex();
        fdx2.readFields(ByteStreams.newDataInput(out.toByteArray()));
        IntTermIterator iter = fdx2.getIntTermIterator("clicked");
        assertTrue(iter.next());
        assertEquals(1, iter.term());
        assertEquals(1, iter.docFreq());
        assertFalse(iter.next());
    }
}
