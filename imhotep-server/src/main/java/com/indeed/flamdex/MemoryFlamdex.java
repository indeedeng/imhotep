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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.GenericIntTermDocIterator;
import com.indeed.flamdex.api.GenericStringTermDocIterator;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.flamdex.fieldcache.IntArrayIntValueLookup;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.flamdex.writer.FlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * @author jsgroth
 */
public final class MemoryFlamdex implements FlamdexReader, FlamdexWriter, FlamdexDocWriter {
    private static final long TREE_MAP_USAGE = 8 + 4 + 4 + 4 + 4 + 12 + 16 + 4 + 4 + 12;
    private static final long TREE_MAP_ENTRY_USAGE = 8 + 4 + 4 + 4 + 4 + 4 + 1;
    //todo jeffp fix this for longs (or don't)
    private static final long INT_2_OBJECT_RB_TREE_MAP_USAGE = 8 + 4 + 4 + 4 + 4 + 12 + 12 + 12 + 1 + 4 + 12 + 4 + 12 + 64 + 4 + 12 + 256 + 4;
    private static final long INT_2_OBJECT_RB_TREE_MAP_ENTRY_USAGE = 8 + 4 + 4 + 4 + 4 + 4;
    private static final long STRING_USAGE = 8 + 4 + 12 + 4 + 4 + 4;
    private static final long INT_ARRAY_LIST_USAGE = 8 + 12 + 4;

    private final SortedMap<String, Long2ObjectSortedMap<IntArrayList>> intFields = Maps.newTreeMap();
    private final SortedMap<String, SortedMap<String, IntArrayList>> stringFields = Maps.newTreeMap();

    private int numDocs;

    private long memoryUsageEstimate = initialMemoryUsageEstimate();

    @Override
    public Collection<String> getIntFields() {
        return intFields.keySet();
    }

    @Override
    public Collection<String> getStringFields() {
        return stringFields.keySet();
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    /*
     * Does nothing
     */
    @Override
    public Path getDirectory() {
        final File tempDir;
        tempDir = Files.createTempDir();
        return tempDir.toPath();
    }

    /*
     * Does nothing
     */
    @Override
    public Path getOutputDirectory() {
        return null;
    }

    @Override
    public void resetMaxDocs(final long numDocs) {
        /* does nothing */
    }

    public MemoryFlamdex setNumDocs(final int numDocs) {
        this.numDocs = numDocs;
        return this;
    }

    @Override
    public DocIdStream getDocIdStream() {
        return new MemoryDocIdStream();
    }

    @Override
    public IntTermIterator getIntTermIterator(final String field) {
        return intFields.containsKey(field) ? new MemoryIntTermIterator(field) : new MemoryIntTermIterator();
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(final String field) {
        return getIntTermIterator(field);
    }

    @Override
    public StringTermIterator getStringTermIterator(final String field) {
        return stringFields.containsKey(field) ? new MemoryStringTermIterator(field) : new MemoryStringTermIterator();
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        return new GenericIntTermDocIterator(getIntTermIterator(field), getDocIdStream());
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        return new GenericStringTermDocIterator(getStringTermIterator(field), getDocIdStream());
    }

    @Override
    public long getIntTotalDocFreq(final String field) {
        return FlamdexUtils.getIntTotalDocFreq(this, field);
    }

    @Override
    public long getStringTotalDocFreq(final String field) {
        return FlamdexUtils.getStringTotalDocFreq(this, field);
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return Collections.emptyList();
    }

    @Override
    public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        return new IntArrayIntValueLookup(FlamdexUtils.cacheIntField(metric, this));
    }

    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long memoryRequired(final String metric) {
        return 0L;
    }

    @Override
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    public IntFieldWriter getIntFieldWriter(final String field) throws IOException {
        return new IntFieldWriter() {
            private final Long2ObjectSortedMap<IntArrayList> terms = new Long2ObjectRBTreeMap<IntArrayList>();
            private IntArrayList currentDocList;
            private long term;

            @Override
            public void nextTerm(final long term) throws IOException {
                if (currentDocList != null && !currentDocList.isEmpty()) {
                    terms.put(this.term, currentDocList);
                }
                this.term = term;
                currentDocList = new IntArrayList();
            }

            @Override
            public void nextDoc(final int doc) throws IOException {
                if (doc >= numDocs) {
                    throw new IllegalArgumentException("invalid doc: doc=" + doc + ", numDocs=" + numDocs);
                }
                currentDocList.add(doc);
            }

            @Override
            public void close() throws IOException {
                if (currentDocList != null && !currentDocList.isEmpty()) {
                    terms.put(term, currentDocList);
                }
                intFields.put(field, terms);
                memoryUsageEstimate += usage(field, terms);
            }
        };
    }

    @Override
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    public StringFieldWriter getStringFieldWriter(final String field) throws IOException {
        return new StringFieldWriter() {
            private final SortedMap<String, IntArrayList> terms = Maps.newTreeMap();
            private IntArrayList currentDocList;
            private String term;

            @Override
            public void nextTerm(final String term) throws IOException {
                if (currentDocList != null && !currentDocList.isEmpty()) {
                    terms.put(this.term, currentDocList);
                }
                this.term = term;
                currentDocList = new IntArrayList();
            }

            @Override
            public void nextDoc(final int doc) throws IOException {
                if (doc >= numDocs) {
                    throw new IllegalArgumentException("invalid doc: doc=" + doc + ", numDocs=" + numDocs);
                }
                currentDocList.add(doc);
            }

            @Override
            public void close() throws IOException {
                if (currentDocList != null && !currentDocList.isEmpty()) {
                    terms.put(term, currentDocList);
                }
                stringFields.put(field, terms);
                memoryUsageEstimate += usage(field, terms);
            }
        };
    }

    @Override
    public void addDocument(final FlamdexDocument doc) {
        final Map<String, LongList> docIntFields = doc.getIntFields();
        for (final Map.Entry<String, LongList> intField : docIntFields.entrySet()) {
            Long2ObjectSortedMap<IntArrayList> myIntTerms = intFields.get(intField.getKey());
            if (myIntTerms == null) {
                intFields.put(intField.getKey(), myIntTerms = new Long2ObjectRBTreeMap<IntArrayList>());
                memoryUsageEstimate += TREE_MAP_ENTRY_USAGE + usage(intField.getKey()) + INT_2_OBJECT_RB_TREE_MAP_USAGE;
            }
            final LongSet seenIntTerms = new LongOpenHashSet();
            final LongList terms = intField.getValue();
            for (int i = 0; i < terms.size(); ++i) {
                final long term = terms.getLong(i);
                if (seenIntTerms.contains(term)) {
                    continue;
                }
                seenIntTerms.add(term);

                IntArrayList docList = myIntTerms.get(term);
                if (docList == null) {
                    myIntTerms.put(term, docList = new IntArrayList());
                    memoryUsageEstimate += INT_2_OBJECT_RB_TREE_MAP_ENTRY_USAGE + INT_ARRAY_LIST_USAGE + (4 * docList.elements().length);
                }
                memoryUsageEstimate -= 4 * docList.elements().length;
                docList.add(numDocs);
                memoryUsageEstimate += 4 * docList.elements().length;
            }
        }

        final Map<String, List<String>> docStringFields = doc.getStringFields();
        for (final Map.Entry<String, List<String>> stringField : docStringFields.entrySet()) {
            SortedMap<String, IntArrayList> myStringTerms = stringFields.get(stringField.getKey());
            if (myStringTerms == null) {
                stringFields.put(stringField.getKey(), myStringTerms = new TreeMap<String, IntArrayList>());
                memoryUsageEstimate += TREE_MAP_ENTRY_USAGE + usage(stringField.getKey()) + TREE_MAP_USAGE;
            }
            final Set<String> seenStringTerms = new HashSet<>();
            final List<String> terms = stringField.getValue();
            for (final String term : terms) {
                if (seenStringTerms.contains(term)) {
                    continue;
                }
                seenStringTerms.add(term);

                IntArrayList docList = myStringTerms.get(term);
                if (docList == null) {
                    myStringTerms.put(term, docList = new IntArrayList());
                    memoryUsageEstimate += TREE_MAP_ENTRY_USAGE + usage(term) + INT_ARRAY_LIST_USAGE + (4 * docList.elements().length);
                }
                memoryUsageEstimate -= 4 * docList.elements().length;
                docList.add(numDocs);
                memoryUsageEstimate += 4 * docList.elements().length;
            }
        }

        ++numDocs;
    }

    @Override
    public void close() throws IOException {
    }

    public MemoryFlamdex() {
        this(false);
    }

    public MemoryFlamdex(final boolean replaceMalformedInput) {
        if (replaceMalformedInput) {
            encoder = Charsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
        } else {
            encoder = Charsets.UTF_8.newEncoder();
        }
    }

    public long getMemoryUsageEstimate() {
        return memoryUsageEstimate;
    }

    public void write(final DataOutput out) throws IOException {
        out.writeInt(numDocs);
        out.writeInt(intFields.size());
        for (final String intField : intFields.keySet()) {
            writeString(out, intField);
        }

        out.writeInt(stringFields.size());
        for (final String stringField : stringFields.keySet()) {
            writeString(out, stringField);
        }

        for (final Long2ObjectSortedMap<IntArrayList> terms : intFields.values()) {
            writeVLong(terms.size(), out);
            long lastTerm = 0;
            for (final Long2ObjectMap.Entry<IntArrayList> e : terms.long2ObjectEntrySet()) {
                final long term = e.getLongKey();
                final IntList docList = e.getValue();

                writeVLong(term - lastTerm, out);
                lastTerm = term;
                writeVLong(docList.size(), out);

                int lastDoc = 0;
                for (int i = 0; i < docList.size(); ++i) {
                    final int doc = docList.getInt(i);
                    writeVLong(doc - lastDoc, out);
                    lastDoc = doc;
                }
            }
        }

        for (final Map<String, IntArrayList> terms : stringFields.values()) {
            writeVLong(terms.size(), out);
            byte[] lastTermBytes = new byte[0];
            int lastTermLength = 0;
            for (final Map.Entry<String, IntArrayList> e : terms.entrySet()) {
                final String term = e.getKey();
                final IntList docList = e.getValue();

                final ByteBuffer encoded = encoder.encode(CharBuffer.wrap(term));
                final byte[] termBytes = encoded.array();
                final int termLength = encoded.limit();
                final int prefixLen = getPrefixLen(lastTermBytes, termBytes, lastTermLength);
                final int newLen = termLength - prefixLen;
                writeVLong(prefixLen, out);
                writeVLong(newLen, out);
                out.write(termBytes, prefixLen, newLen);
                lastTermBytes = termBytes;
                lastTermLength = termLength;

                writeVLong(docList.size(), out);
                int lastDoc = 0;
                for (int i = 0; i < docList.size(); ++i) {
                    final int doc = docList.getInt(i);
                    writeVLong(doc - lastDoc, out);
                    lastDoc = doc;
                }
            }
        }
    }

    public void readFields(final DataInput in) throws IOException {
        numDocs = in.readInt();

        final int numIntFields = in.readInt();
        intFields.clear();

        for (int z = 0; z < numIntFields; ++z) {
            final String intField = readString(in);
            intFields.put(intField, new Long2ObjectRBTreeMap<IntArrayList>());
        }

        final int numStringFields = in.readInt();
        stringFields.clear();

        for (int z = 0; z < numStringFields; ++z) {
            final String stringField = readString(in);
            stringFields.put(stringField, new TreeMap<String, IntArrayList>());
        }

        for (final Long2ObjectMap<IntArrayList> terms : intFields.values()) {
            final long numTerms = readVLong(in);
            long term = 0;
            for (long l = 0; l < numTerms; l++) {
                final long termDelta = readVLong(in);

                term += termDelta;
                final int docFreq = (int) readVLong(in);
                final IntArrayList docList = new IntArrayList(docFreq);
                terms.put(term, docList);

                int doc = 0;
                for (int x = 0; x < docFreq; ++x) {
                    final int docDelta = (int) readVLong(in);
                    doc += docDelta;
                    docList.add(doc);
                }
            }
        }

        final CharsetDecoder decoder = Charsets.UTF_8.newDecoder();

        for (final Map<String, IntArrayList> terms : stringFields.values()) {
            final long numTerms = readVLong(in);
            byte[] term = new byte[10];
            ByteBuffer termBuf = ByteBuffer.wrap(term);
            for (long l = 0; l < numTerms; l++) {
                final long prefixLen = readVLong(in);

                final int newLen = (int) readVLong(in);
                final int termLen = (int) prefixLen + newLen;
                if (termLen > term.length) {
                    term = Arrays.copyOf(term, Math.max(termLen, 2 * term.length));
                    termBuf = ByteBuffer.wrap(term);
                }
                if (newLen > 0) {
                    in.readFully(term, (int) prefixLen, newLen);
                }

                final String termStr = decoder.decode((ByteBuffer) termBuf.position(0).limit(termLen)).toString();
                final int docFreq = (int) readVLong(in);
                final IntArrayList docList = new IntArrayList(docFreq);
                terms.put(termStr, docList);
                int doc = 0;
                for (int i = 0; i < docFreq; ++i) {
                    final int docDelta = (int) readVLong(in);
                    doc += docDelta;
                    docList.add(doc);
                }
            }
        }

        memoryUsageEstimate = initialMemoryUsageEstimate();
        for (final Map.Entry<String, Long2ObjectSortedMap<IntArrayList>> e : intFields.entrySet()) {
            memoryUsageEstimate += usage(e.getKey(), e.getValue());
        }
        for (final Map.Entry<String, SortedMap<String, IntArrayList>> e : stringFields.entrySet()) {
            memoryUsageEstimate += usage(e.getKey(), e.getValue());
        }
    }

    private static long initialMemoryUsageEstimate() {
        long size = 20;
        size += 2 * TREE_MAP_USAGE;
        return size;
    }

    private static long usage(final String intField, final Long2ObjectMap<IntArrayList> terms) {
        long size = TREE_MAP_ENTRY_USAGE;
        size += usage(intField);
        size += usage(terms);
        return size;
    }

    private static long usage(final String stringField, final SortedMap<String, IntArrayList> terms) {
        long size = TREE_MAP_ENTRY_USAGE;
        size += usage(stringField);
        size += usage(terms);
        return size;
    }

    private static long usage(final String s) {
        return STRING_USAGE + 2 * s.length();
    }

    private static long usage(final Long2ObjectMap<IntArrayList> map) {
        long size = INT_2_OBJECT_RB_TREE_MAP_USAGE;
        size += map.size() * INT_2_OBJECT_RB_TREE_MAP_ENTRY_USAGE;
        for (final IntArrayList list : map.values()) {
            size += INT_ARRAY_LIST_USAGE + 4 * list.elements().length;
        }
        return size;
    }

    private static long usage(final SortedMap<String, IntArrayList> map) {
        long size = TREE_MAP_USAGE;
        for (final Map.Entry<String, IntArrayList> e : map.entrySet()) {
            size += TREE_MAP_ENTRY_USAGE;
            size += usage(e.getKey());
            size += INT_ARRAY_LIST_USAGE + 4 * e.getValue().elements().length;
        }
        return size;
    }

    // DO NOT USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING
    public static FlamdexReader streamer(final DataInput in) throws IOException {
        final int numDocs = in.readInt();

        final int numIntFields = in.readInt();
        final SortedSet<String> intFields = Sets.newTreeSet();

        for (int i = 0; i < numIntFields; ++i) {
            intFields.add(readString(in));
        }

        final int numStringFields = in.readInt();
        final SortedSet<String> stringFields = Sets.newTreeSet();
        for (int i = 0; i < numStringFields; ++i) {
            stringFields.add(readString(in));
        }

        final CharsetDecoder decoder = Charsets.UTF_8.newDecoder();

        return new FlamdexReader() {
            long intTerm;
            byte[] stringTermBytes = new byte[100];
            ByteBuffer stringTermBuf = ByteBuffer.wrap(stringTermBytes);
            String stringTerm;
            int stringTermLen;
            int docFreq;

            int[] docList = new int[1000];

            @Override
            public Collection<String> getIntFields() {
                return intFields;
            }

            @Override
            public Collection<String> getStringFields() {
                return stringFields;
            }

            @Override
            public int getNumDocs() {
                return numDocs;
            }

            /*
             * Does nothing
             */
            @Override
            public Path getDirectory() {
                return Paths.get(".");
            }

            @Override
            public DocIdStream getDocIdStream() {
                return new DocIdStream() {
                    int[] myDocList = new int[1000];
                    int pos;
                    int myDocListSize;

                    @Override
                    public void reset(final TermIterator term) {
                        if (docFreq > myDocList.length) {
                            myDocList = new int[Math.max(docFreq, 2 * myDocList.length)];
                        }
                        System.arraycopy(docList, 0, myDocList, 0, docFreq);
                        pos = 0;
                        myDocListSize = docFreq;
                    }

                    @Override
                    public int fillDocIdBuffer(final int[] docIdBuffer) {
                        final int n = Math.min(myDocListSize - pos, docIdBuffer.length);
                        System.arraycopy(myDocList, pos, docIdBuffer, 0, n);
                        pos += n;
                        return n;
                    }

                    @Override
                    public void close() {
                    }
                };
            }

            @Override
            public IntTermIterator getIntTermIterator(final String field) {
                final long numTerms;
                try {
                    numTerms = readVLong(in);
                } catch (final IOException e) {
                    throw Throwables.propagate(e);
                }
                intTerm = 0;
                return new IntTermIterator() {

                    long termIndex = 0;

                    @Override
                    public void reset(final long term) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long term() {
                        return intTerm;
                    }

                    @Override
                    public boolean next() {
                        if (termIndex >= numTerms) {
                            return false;
                        }
                        termIndex++;
                        try {
                            final long termDelta = readVLong(in);

                            intTerm += termDelta;
                            docFreq = (int) readVLong(in);
                            if (docFreq > docList.length) {
                                docList = new int[Math.max(docFreq, 2 * docList.length)];
                            }
                            int doc = 0;
                            for (int i = 0; i < docFreq; ++i) {
                                doc += (int) readVLong(in);
                                docList[i] = doc;
                            }
                            return true;
                        } catch (final IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public int docFreq() {
                        return docFreq;
                    }

                    @Override
                    public void close() {
                    }
                };
            }

            @Override
            public IntTermIterator getUnsortedIntTermIterator(final String field) {
                return getIntTermIterator(field);
            }

            @Override
            public StringTermIterator getStringTermIterator(final String field) {
                final long numTerms;
                try {
                    numTerms = readVLong(in);
                } catch (final IOException e) {
                    throw Throwables.propagate(e);
                }
                stringTermLen = 0;
                return new StringTermIterator() {

                    long termIndex = 0;

                    @Override
                    public void reset(final String term) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String term() {
                        return stringTerm;
                    }

                    @Override
                    public boolean next() {
                        if (termIndex >= numTerms) {
                            return false;
                        }
                        termIndex++;
                        try {
                            final long prefixLen = readVLong(in);

                            final int newLen = (int) readVLong(in);
                            stringTermLen = (int) prefixLen + newLen;
                            if (stringTermLen > stringTermBytes.length) {
                                stringTermBytes = Arrays.copyOf(stringTermBytes, Math.max(stringTermLen, 2 * stringTermBytes.length));
                                stringTermBuf = ByteBuffer.wrap(stringTermBytes);
                            }
                            if (newLen > 0) {
                                in.readFully(stringTermBytes, (int) prefixLen, newLen);
                            }

                            stringTerm = decoder.decode((ByteBuffer) stringTermBuf.position(0).limit(stringTermLen)).toString();
                            docFreq = (int) readVLong(in);
                            if (docFreq > docList.length) {
                                docList = new int[Math.max(docFreq, 2 * docList.length)];
                            }

                            int doc = 0;
                            for (int i = 0; i < docFreq; ++i) {
                                doc += (int) readVLong(in);
                                docList[i] = doc;
                            }

                            return true;
                        } catch (final IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public int docFreq() {
                        return docFreq;
                    }

                    @Override
                    public void close() {
                    }
                };
            }

            @Override
            public IntTermDocIterator getIntTermDocIterator(final String field) {
                return new GenericIntTermDocIterator(getIntTermIterator(field), getDocIdStream());
            }

            @Override
            public StringTermDocIterator getStringTermDocIterator(final String field) {
                return new GenericStringTermDocIterator(getStringTermIterator(field), getDocIdStream());
            }

            @Override
            public long getIntTotalDocFreq(final String field) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getStringTotalDocFreq(final String field) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Collection<String> getAvailableMetrics() {
                throw new UnsupportedOperationException();
            }

            @Override
            public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
                throw new UnsupportedOperationException();
            }

            public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long memoryRequired(final String metric) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws IOException {
                if (in instanceof Closeable) {
                    ((Closeable) in).close();
                }
            }
        };
    }

    public MemoryFlamdex shallowCopy() {
        final MemoryFlamdex ret = new MemoryFlamdex();
        ret.numDocs = numDocs;
        ret.intFields.putAll(intFields);
        ret.stringFields.putAll(stringFields);
        return ret;
    }

    private static int getPrefixLen(final byte[] a, final byte[] b, final int n) {
        for (int i = 0; i < n; ++i) {
            if (a[i] != b[i]) {
                return i;
            }
        }
        return n;
    }

    private interface MemoryTermIterator extends TermIterator {
        IntList getDocList();
    }

    private class MemoryIntTermIterator implements MemoryTermIterator, IntTermIterator {
        private final Long2ObjectSortedMap<IntArrayList> map;
        private Iterator<Long2ObjectMap.Entry<IntArrayList>> keys;

        private long term;
        private IntList docList;

        private MemoryIntTermIterator(final String field) {
            map = intFields.get(field);
            keys = map.long2ObjectEntrySet().iterator();
        }

        private MemoryIntTermIterator() {
            map = new Long2ObjectRBTreeMap<>();
            keys = map.long2ObjectEntrySet().iterator();
        }

        @Override
        public void reset(final long term) {
            keys = map.tailMap(term).long2ObjectEntrySet().iterator();
        }

        @Override
        public long term() {
            return term;
        }

        @Override
        public IntList getDocList() {
            return docList;
        }

        @Override
        public boolean next() {
            if (!keys.hasNext()) {
                return false;
            }
            final Long2ObjectMap.Entry<IntArrayList> e = keys.next();
            term = e.getLongKey();
            docList = e.getValue();
            // noinspection SimplifiableIfStatement
            if (docList.isEmpty()) {
                return next();
            }
            return true;
        }

        @Override
        public int docFreq() {
            return docList.size();
        }

        @Override
        public void close() {
        }
    }

    private class MemoryStringTermIterator implements MemoryTermIterator, StringTermIterator {
        private final SortedMap<String, IntArrayList> map;
        private Iterator<Map.Entry<String, IntArrayList>> keys;

        private String term;
        private IntList docList;

        private MemoryStringTermIterator(final String field) {
            map = stringFields.get(field);
            keys = map.entrySet().iterator();
        }

        private MemoryStringTermIterator() {
            map = new TreeMap<>();
            keys = map.entrySet().iterator();
        }

        @Override
        public void reset(final String term) {
            keys = map.tailMap(term).entrySet().iterator();
        }

        @Override
        public String term() {
            return term;
        }

        @Override
        public IntList getDocList() {
            return docList;
        }

        @Override
        public boolean next() {
            if (!keys.hasNext()) {
                return false;
            }
            final Map.Entry<String, IntArrayList> e = keys.next();
            term = e.getKey();
            docList = e.getValue();
            // noinspection SimplifiableIfStatement
            if (docList.isEmpty()) {
                return next();
            }
            return true;
        }

        @Override
        public int docFreq() {
            return docList.size();
        }

        @Override
        public void close() {
        }
    }

    private class MemoryDocIdStream implements DocIdStream {
        private IntList docList;
        private int index;

        @Override
        public void reset(final TermIterator term) {
            if (!(term instanceof MemoryTermIterator)) {
                throw new IllegalArgumentException("invalid term iterator");
            }
            internalReset((MemoryTermIterator) term);
        }

        private void internalReset(final MemoryTermIterator term) {
            docList = term.getDocList();
            index = 0;
        }

        @Override
        public int fillDocIdBuffer(final int[] docIdBuffer) {
            final int n = Math.min(docIdBuffer.length, docList.size() - index);
            for (int i = 0; i < n; ++i) {
                docIdBuffer[i] = docList.getInt(index++);
            }
            return n;
        }

        @Override
        public void close() {
        }
    }

    private final CharsetEncoder encoder;

    private static final ThreadLocal<CharsetEncoder> ENCODER = new ThreadLocal<CharsetEncoder>() {
        @Override
        protected CharsetEncoder initialValue() {
            return Charsets.UTF_8.newEncoder();
        }
    };

    private static final ThreadLocal<CharsetDecoder> DECODER = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return Charsets.UTF_8.newDecoder();
        }
    };

    private static String readString(final DataInput in) throws IOException {
        final int len = (int) readVLong(in);
        final byte[] bytes = new byte[len];
        in.readFully(bytes);
        return DECODER.get().decode(ByteBuffer.wrap(bytes)).toString();
    }

    private static void writeString(final DataOutput out, final String s) throws IOException {
        final ByteBuffer encoded = ENCODER.get().encode(CharBuffer.wrap(s));
        final int len = encoded.limit();
        writeVLong(len, out);
        out.write(encoded.array(), 0, len);
    }

    /*
     * the following methods were forked from org.apache.hadoop.io.WritableUtils
     */

    private static void writeVLong(long i, final DataOutput out) throws IOException {
        if (i >= -112 && i <= 127) {
            out.write((int) (i & 0xFF));
            return;
        }

        int len = -112;
        if (i < 0) {
            i = ~i;
            len = -120;
        }

        long tmp = i;
        while (tmp != 0) {
            tmp = tmp >> 8;
            len--;
        }

        out.write(len & 0xFF);

        len = (len < -120) ? -(len + 120) : -(len + 112);

        for (int idx = len; idx != 0; idx--) {
            final int shiftbits = (idx - 1) * 8;
            final long mask = 0xFFL << shiftbits;
            out.write((int) ((i & mask) >>> shiftbits));
        }
    }

    private static long readVLong(final DataInput in) throws IOException {
        final byte firstByte = in.readByte();
        final int len = decodeVIntSize(firstByte);
        if (len == 1) {
            return firstByte;
        }
        long i = 0;
        for (int idx = 0; idx < len - 1; idx++) {
            final int b = in.readUnsignedByte();
            i = i << 8;
            i = i | b;
        }
        return (isNegativeVInt(firstByte) ? (~i) : i);
    }

    private static boolean isNegativeVInt(final byte value) {
        return value < -120 || (value >= -112 && value < 0);
    }

    private static int decodeVIntSize(final byte value) {
        if (value >= -112) {
            return 1;
        } else if (value < -120) {
            return -119 - value;
        }
        return -111 - value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MemoryFlamdex that = (MemoryFlamdex) o;

        if (numDocs != that.numDocs) {
            return false;
        }
        if (intFields != null ? !intFields.equals(that.intFields) : that.intFields != null) {
            return false;
        }
        if (stringFields != null ? !stringFields.equals(that.stringFields) : that.stringFields != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = intFields != null ? intFields.hashCode() : 0;
        result = 31 * result + (stringFields != null ? stringFields.hashCode() : 0);
        result = 31 * result + numDocs;
        return result;
    }
}
