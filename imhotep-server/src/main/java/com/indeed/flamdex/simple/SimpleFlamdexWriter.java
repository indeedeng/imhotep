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

import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.util.core.sort.Quicksortable;
import com.indeed.util.core.sort.Quicksortables;
import com.indeed.util.io.Files;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.StringSerializer;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.flamdex.writer.FlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.lsmtree.core.Generation;
import com.indeed.lsmtree.core.ImmutableBTreeIndex;
import com.indeed.util.mmap.IntArray;
import com.indeed.util.mmap.MMapBuffer;
import it.unimi.dsi.fastutil.IndirectPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectHeapSemiIndirectPriorityQueue;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * @author jsgroth
 */
public class SimpleFlamdexWriter implements java.io.Closeable, FlamdexWriter {
    private static final Logger log = Logger.getLogger(SimpleFlamdexWriter.class);

    public static final int FORMAT_VERSION = 0;

    private static final int DOC_ID_BUFFER_SIZE = 32;

    private static final int BLOCK_SIZE = 64;    

    private final String outputDirectory;
    private long maxDocs;

    private final boolean writeBTreesOnClose;

    private final Set<String> intFields;
    private final Set<String> stringFields;

    public SimpleFlamdexWriter(String outputDirectory, long numDocs) throws IOException {
        this(outputDirectory, numDocs, true, true);
    }

    public SimpleFlamdexWriter(String outputDirectory, long numDocs, boolean create) throws IOException {
        this(outputDirectory, numDocs, create, true);
    }

    public SimpleFlamdexWriter(String outputDirectory, long numDocs, boolean create, boolean writeBTreesOnClose) throws IOException {
        this.outputDirectory = outputDirectory;
        this.maxDocs = numDocs;
        this.writeBTreesOnClose = writeBTreesOnClose;
        if (create) {
            if (new File(outputDirectory).exists()) {
                deleteIndex(outputDirectory);
            } else if (!new File(outputDirectory).mkdirs()) {
                throw new IOException("unable to create directory at " + outputDirectory);
            }
            intFields = new HashSet<String>();
            stringFields = new HashSet<String>();
        } else {
            final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(outputDirectory);
            if (metadata.numDocs != numDocs) {
                throw new IllegalArgumentException("numDocs (" + numDocs + ") does not match numDocs in existing index (" + metadata.numDocs + ")");
            }
            intFields = new HashSet<String>(metadata.intFields);
            stringFields = new HashSet<String>(metadata.stringFields);
        }
    }
    
    @Override
    public String getOutputDirectory() {
        return this.outputDirectory;
    }
    
    public void resetMaxDocs(long numDocs) {
        this.maxDocs = numDocs;
    }

    @Override
    public IntFieldWriter getIntFieldWriter(String field) throws FileNotFoundException {
        return getIntFieldWriter(field, false);
    }

    public IntFieldWriter getIntFieldWriter(String field, boolean blowAway) throws FileNotFoundException {
        if (!blowAway && intFields.contains(field)) {
            throw new IllegalArgumentException("already added int field "+field);
        }
        intFields.add(field);
        return SimpleIntFieldWriter.open(outputDirectory, field, maxDocs, writeBTreesOnClose);
    }

    @Override
    public StringFieldWriter getStringFieldWriter(String field) throws FileNotFoundException {
        return getStringFieldWriter(field, false);
    }

    public StringFieldWriter getStringFieldWriter(String field, boolean blowAway) throws FileNotFoundException {
        if (!blowAway && stringFields.contains(field)) {
            throw new IllegalArgumentException("already added string field "+field);
        }
        stringFields.add(field);
        return SimpleStringFieldWriter.open(outputDirectory, field, maxDocs, writeBTreesOnClose);
    }

    @Override
    public void close() throws IOException {
        final List<String> intFieldsList = new ArrayList<String>(intFields);
        Collections.sort(intFieldsList);

        final List<String> stringFieldsList = new ArrayList<String>(stringFields);
        Collections.sort(stringFieldsList);

        final FlamdexMetadata metadata = new FlamdexMetadata((int)maxDocs, intFieldsList, stringFieldsList, FORMAT_VERSION);
        FlamdexMetadata.writeMetadata(outputDirectory, metadata);
    }

    public static void writeIntBTree(String directory, String intField, File btreeDir) throws IOException {
        final String termsFilename = Files.buildPath(directory, SimpleIntFieldWriter.getTermsFilename(intField));
        if (!new File(termsFilename).exists() || new File(termsFilename).length() == 0L) return;
        final CountingInputStream termsList = new CountingInputStream(new BufferedInputStream(new FileInputStream(termsFilename), 65536));
        try {
            ImmutableBTreeIndex.Writer.write(btreeDir, new AbstractIterator<Generation.Entry<Long, LongPair>>() {
                private long lastTerm = 0;
                private long lastTermDocOffset = 0L;
                private long lastTermFileOffset = 0L;

                private long key;
                private LongPair value;

                @Override
                protected Generation.Entry<Long, LongPair> computeNext() {
                    try {
                        if (!nextTerm()) return endOfData();

                        key = lastTerm;
                        value = new LongPair(lastTermFileOffset, lastTermDocOffset);

                        for (int i = 0; i < BLOCK_SIZE-1; ++i) {
                            if (!nextTerm()) {
                                break;
                            }
                        }

                        return Generation.Entry.create(key, value);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                private boolean nextTerm() throws IOException {
                    final long termDelta;
                    //sorry
                    try {
                        termDelta = FlamdexUtils.readVLong(termsList);
                    } catch (EOFException e) {
                        return false;
                    }

                    lastTerm += termDelta;

                    final long offsetDelta = FlamdexUtils.readVLong(termsList);
                    lastTermDocOffset += offsetDelta;

                    lastTermFileOffset = termsList.getCount();

                    FlamdexUtils.readVLong(termsList); // termDocFreq

                    return true;
                }
            }, new LongSerializer(), new LongPairSerializer(), 65536, false);
        } finally {
            termsList.close();
        }
    }

    public static void writeStringBTree(String directory, String stringField, File btreeDir) throws IOException {
        final String termsFilename = Files.buildPath(directory, SimpleStringFieldWriter.getTermsFilename(stringField));        
        if (!new File(termsFilename).exists() || new File(termsFilename).length() == 0L) return;
        final CountingInputStream termsList = new CountingInputStream(new BufferedInputStream(new FileInputStream(termsFilename), 65536));
        try {
            ImmutableBTreeIndex.Writer.write(btreeDir, new AbstractIterator<Generation.Entry<String, LongPair>>() {
                private String key;
                private LongPair value;

                private byte[] lastTerm = new byte[10];
                private int lastTermLen = 0;
                private long lastTermDocOffset = 0L;
                private long lastTermFileOffset = 0L;

                @Override
                public Generation.Entry<String, LongPair> computeNext() {
                    try {
                        if (!nextTerm()) return endOfData();

                        key = new String(lastTerm, 0, lastTermLen, Charsets.UTF_8);
                        value = new LongPair(lastTermFileOffset, lastTermDocOffset);

                        for (int i = 0; i < BLOCK_SIZE - 1; ++i) {
                            if (!nextTerm()) {
                                break;
                            }
                        }

                        return Generation.Entry.create(key, value);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                private boolean nextTerm() throws IOException {
                    final int removeLen;
                    //sorry
                    try {
                        removeLen = (int) FlamdexUtils.readVLong(termsList);
                    } catch (EOFException e) {
                        return false;
                    }

                    final int newLen = (int)FlamdexUtils.readVLong(termsList);

                    lastTerm = ensureCapacity(lastTerm, lastTermLen - removeLen + newLen);
                    ByteStreams.readFully(termsList, lastTerm, lastTermLen - removeLen, newLen);
                    lastTermLen = lastTermLen - removeLen + newLen;

                    final long offsetDelta = FlamdexUtils.readVLong(termsList);
                    lastTermDocOffset += offsetDelta;

                    lastTermFileOffset = termsList.getCount();

                    FlamdexUtils.readVLong(termsList); // termDocFreq

                    return true;
                }
            }, new StringSerializer(), new LongPairSerializer(), 65536, false);
        } finally {
            termsList.close();
        }
    }

    private static byte[] ensureCapacity(final byte[] a, final int capacity) {
        return capacity <= a.length ? a : Arrays.copyOf(a, Math.max(2*a.length, capacity));
    }

    public static void writeFlamdex(final FlamdexReader fdx, final FlamdexWriter w) throws IOException {
        final DocIdStream dis = fdx.getDocIdStream();
        final int[] docIdBuf = new int[DOC_ID_BUFFER_SIZE];

        for (final String intField : fdx.getIntFields()) {
            final IntFieldWriter ifw = w.getIntFieldWriter(intField);
            final IntTermIterator iter = fdx.getIntTermIterator(intField);
            while (iter.next()) {
                ifw.nextTerm(iter.term());
                dis.reset(iter);
                while (true) {
                    final int n = dis.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        ifw.nextDoc(docIdBuf[i]);
                    }
                    if (n < docIdBuf.length) break;
                }
            }
            iter.close();
            ifw.close();
        }

        for (final String stringField : fdx.getStringFields()) {
            final StringFieldWriter sfw = w.getStringFieldWriter(stringField);
            final StringTermIterator iter = fdx.getStringTermIterator(stringField);
            while (iter.next()) {
                sfw.nextTerm(iter.term());
                dis.reset(iter);
                while (true) {
                    final int n = dis.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        sfw.nextDoc(docIdBuf[i]);
                    }
                    if (n < docIdBuf.length) break;
                }
            }
            iter.close();
            sfw.close();
        }

        dis.close();
        w.close();
    }

    public static void merge(Collection<? extends FlamdexReader> readers, FlamdexWriter w) throws IOException {
        merge(readers.toArray(new FlamdexReader[readers.size()]), w);
    }

    public static void merge(FlamdexReader[] readers, FlamdexWriter w) throws IOException {
        final DocIdStream[] docIdStreams = new DocIdStream[readers.length];
        final int[] segmentStartDocs = new int[readers.length];
        int totalNumDocs = 0;
        for (int i = 0; i < readers.length; ++i) {
            docIdStreams[i] = readers[i].getDocIdStream();
            segmentStartDocs[i] = totalNumDocs;
            totalNumDocs += readers[i].getNumDocs();
        }

        log.info("merging " + readers.length + " readers with a total of " + totalNumDocs + " docs");

        final int[] indexBuf = new int[readers.length];
        final int[] docIdBuf = new int[64];

        for (final String intField : mergeIntFields(readers)) {
            final IntFieldWriter ifw = w.getIntFieldWriter(intField);

            final IntTermIteratorWrapper[] iterators = new IntTermIteratorWrapper[readers.length];
            final IndirectPriorityQueue<IntTermIteratorWrapper> pq = new ObjectHeapSemiIndirectPriorityQueue<IntTermIteratorWrapper>(iterators, iterators.length);
            for (int i = 0; i < readers.length; ++i) {
                if (!readers[i].getIntFields().contains(intField)) continue;
                final IntTermIterator it = readers[i].getIntTermIterator(intField);
                if (it.next()) {
                    iterators[i] = new IntTermIteratorWrapper(it, i);
                    pq.enqueue(i);
                } else {
                    it.close();
                }
            }

            while (!pq.isEmpty()) {
                final long term = iterators[pq.first()].it.term();
                int numIndexes = 0;
                IntTermIteratorWrapper wrap;
                while (!pq.isEmpty() && (wrap = iterators[pq.first()]).it.term() == term) {
                    final int index = wrap.index;
                    docIdStreams[index].reset(wrap.it);
                    indexBuf[numIndexes++] = index;
                    if (wrap.it.next()) {
                        pq.changed();
                    } else {
                        wrap.it.close();
                        pq.dequeue();
                    }
                }

                ifw.nextTerm(term);
                for (int i = 0; i < numIndexes; ++i) {
                    final int index = indexBuf[i];
                    final int startDoc = segmentStartDocs[index];
                    final DocIdStream dis = docIdStreams[index];
                    while (true) {
                        final int n = dis.fillDocIdBuffer(docIdBuf);

                        for (int j = 0; j < n; ++j) {
                            ifw.nextDoc(docIdBuf[j]+startDoc);
                        }

                        if (n < docIdBuf.length) break;
                    }
                }
            }

            ifw.close();
        }

        for (final String stringField : mergeStringFields(readers)) {
            final StringFieldWriter sfw = w.getStringFieldWriter(stringField);

            final StringTermIteratorWrapper[] iterators = new StringTermIteratorWrapper[readers.length];
            final IndirectPriorityQueue<StringTermIteratorWrapper> pq = new ObjectHeapSemiIndirectPriorityQueue<StringTermIteratorWrapper>(iterators, iterators.length);
            for (int i = 0; i < readers.length; ++i) {
                if (!readers[i].getStringFields().contains(stringField)) continue;
                final StringTermIterator it = readers[i].getStringTermIterator(stringField);
                if (it.next()) {
                    iterators[i] = new StringTermIteratorWrapper(it, i);
                    pq.enqueue(i);
                } else {
                    it.close();
                }
            }

            while (!pq.isEmpty()) {
                final String term = iterators[pq.first()].it.term();
                int numIndexes = 0;
                StringTermIteratorWrapper wrap;
                while (!pq.isEmpty() && (wrap = iterators[pq.first()]).it.term().equals(term)) {
                    final int index = wrap.index;
                    docIdStreams[index].reset(wrap.it);
                    indexBuf[numIndexes++] = index;
                    if (wrap.it.next()) {
                        pq.changed();
                    } else {
                        wrap.it.close();
                        pq.dequeue();
                    }
                }

                sfw.nextTerm(term);
                for (int i = 0; i < numIndexes; ++i) {
                    final int index = indexBuf[i];
                    final int startDoc = segmentStartDocs[index];
                    final DocIdStream dis = docIdStreams[index];
                    while (true) {
                        final int n = dis.fillDocIdBuffer(docIdBuf);

                        for (int j = 0; j < n; ++j) {
                            sfw.nextDoc(docIdBuf[j]+startDoc);
                        }

                        if (n < docIdBuf.length) break;
                    }
                }
            }

            sfw.close();
        }

        for (final DocIdStream dis : docIdStreams) {
            dis.close();
        }
    }

    private static Set<String> mergeIntFields(FlamdexReader[] readers) {
        final Set<String> ret = new TreeSet<String>();
        for (final FlamdexReader reader : readers) {
            ret.addAll(reader.getIntFields());
        }
        return ret;
    }

    private static Set<String> mergeStringFields(FlamdexReader[] readers) {
        final Set<String> ret = new TreeSet<String>();
        for (final FlamdexReader reader : readers) {
            ret.addAll(reader.getStringFields());
        }
        return ret;
    }

    private static final class IntTermIteratorWrapper implements Comparable<IntTermIteratorWrapper> {
        private final IntTermIterator it;
        private final int index;

        private IntTermIteratorWrapper(IntTermIterator it, int index) {
            this.it = it;
            this.index = index;
        }

        @Override
        public int compareTo(IntTermIteratorWrapper o) {
            final int cmp;
            return (cmp = Longs.compare(it.term(), o.it.term())) != 0 ? cmp : Ints.compare(index, o.index);
        }
    }

    private static final class StringTermIteratorWrapper implements Comparable<StringTermIteratorWrapper> {
        private final StringTermIterator it;
        private final int index;

        private StringTermIteratorWrapper(StringTermIterator it, int index) {
            this.it = it;
            this.index = index;
        }

        @Override
        public int compareTo(StringTermIteratorWrapper o) {
            final int c = it.term().compareTo(o.it.term());
            return c != 0 ? c : index - o.index;
        }
    }

    public static void addField(String dir, String fieldName, FlamdexReader r, final long[] cache) throws IOException {
        final File tempFile = new File(dir, "temp-" + fieldName + "-" + UUID.randomUUID() + ".intarray.bin");
        try {
            final MMapBuffer buffer = new MMapBuffer(tempFile, 0, 4 * cache.length, FileChannel.MapMode.READ_WRITE, ByteOrder.nativeOrder());
            try {
                final IntArray indices = buffer.memory().intArray(0, cache.length);
                for (int i = 0; i < cache.length; ++i) {
                    indices.set(i, i);
                }
                log.debug("sorting");
                Quicksortables.sort(new Quicksortable() {
                    @Override
                    public void swap(int i, int j) {
                        final int t = indices.get(i);
                        indices.set(i, indices.get(j));
                        indices.set(j, t);
                    }

                    @Override
                    public int compare(int i, int j) {
                        final long ii = cache[indices.get(i)];
                        final long ij = cache[indices.get(j)];
                        return ii < ij ? -1 : ii > ij ? 1 : indices.get(i) < indices.get(j) ? -1 : indices.get(i) > indices.get(j) ? 1 : 0;
                    }
                }, cache.length);

                log.debug("writing field " + fieldName);
                final SimpleFlamdexWriter w = new SimpleFlamdexWriter(dir, r.getNumDocs(), false);
                final IntFieldWriter ifw = w.getIntFieldWriter(fieldName, true);
                long prev = 0;
                boolean prevInitialized = false;
                for (int i = 0; i < cache.length; ++i) {
                    final long cur = cache[indices.get(i)];
                    if (!prevInitialized || cur != prev) {
                        ifw.nextTerm(cur);
                        prev = cur;
                        prevInitialized = true;
                    }
                    ifw.nextDoc(indices.get(i));
                }

                ifw.close();
                w.close();
            } finally {
                try {
                    buffer.close();
                } catch (IOException e) {
                    log.error("error closing MMapBuffer", e);
                }
            }
        } finally {
            if (!tempFile.delete()) {
                log.warn("unable to delete temp file " + tempFile);
            }
        }
    }

    public static void addField(String indexDir, String newFieldName, FlamdexReader docReader, final String[] values)  throws IOException {
        final int[] indices = new int[docReader.getNumDocs()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        log.debug("sorting");
        Quicksortables.sort(new Quicksortable() {
            @Override
            public void swap(int i, int j) {
                Quicksortables.swap(indices, i, j);
            }

            @Override
            public int compare(int i, int j) {
                // Sorting logic: Primarily by value (String), secondarily by document ID (indices[i])
                final String left = values[indices[i]];
                final String right = values[indices[j]];
                if (left.compareTo(right) < 0) {
                    return -1;
                } else if (left.compareTo(right) > 0) {
                    return 1;
                } else { // left == right
                    if (indices[i] < indices[j]) {
                        return -1;
                    } else if (indices[i] > indices[j]) {
                        return 1;
                    } else {
                        return 0; // Both value & doc ID match
                    }
                }
            }
        }, values.length);

        log.debug("writing field " + newFieldName);
        final SimpleFlamdexWriter w = new SimpleFlamdexWriter(indexDir, docReader.getNumDocs(), false);
        final StringFieldWriter sfw = w.getStringFieldWriter(newFieldName, true);
        final IntArrayList docList = new IntArrayList();
        docList.add(indices[0]);
        for (int i = 1; i < indices.length; ++i) {
            final String prev = values[indices[i - 1]];
            final String cur = values[indices[i]];
            if (cur.compareTo(prev) != 0) {
                sfw.nextTerm(prev);
                for (int j = 0; j < docList.size(); ++j) {
                    sfw.nextDoc(docList.getInt(j));
                }
                docList.clear();
            }
            docList.add(indices[i]);
        }
        if (docList.size() > 0) {
            sfw.nextTerm(values[indices[indices.length - 1]]);
            for (int j = 0; j < docList.size(); ++j) {
                sfw.nextDoc(docList.getInt(j));
            }
        }

        sfw.close();
        w.close();
    }

    public static void deleteIndex(final String dir) throws IOException {
        final File[] files = new File(dir).listFiles(new SimpleFlamdexFileFilter());
        if (files != null) {
            for (final File f : files) {
                if (f.isDirectory()) {
                    for (final File sub : f.listFiles()) {
                        if (!sub.delete()) {
                            throw new IOException("unable to delete file in index sub directory: " + sub.getAbsolutePath());
                        }
                    }
                }
                if (!f.delete()) {
                    throw new IOException("unable to delete file in index directory: " + f.getAbsolutePath());
                }
            }
        }
    }
}
