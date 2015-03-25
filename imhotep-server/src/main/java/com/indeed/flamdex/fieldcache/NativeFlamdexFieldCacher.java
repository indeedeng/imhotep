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

import com.google.common.annotations.VisibleForTesting;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.MMapFastBitSet;
import com.indeed.flamdex.simple.SimpleIntTermIterator;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.UUID;

/**
 * @author jsgroth
 */
public enum NativeFlamdexFieldCacher {
    LONG {
        @Override
        public long memoryRequired(int numDocs) {
            return 8L * numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(SimpleIntTermIterator iter,
                                                       int numDocs) throws IOException {
            long[] backingArray = new long[numDocs];

            long[] terms = new long[BUFFER_SIZE];
            int[] n_docs = new int[BUFFER_SIZE];
            long[] offsets = new long[BUFFER_SIZE];
            long address = iter.getDocListAddress();
            while (true) {
                int j;
                boolean hasNext = iter.next();
                for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                    terms[j] = iter.term();
                    n_docs[j] = iter.docFreq();
                    offsets[j] = iter.getOffset();
                    hasNext = iter.next();
                }
                nativeCacheLongMetricValuesInArray(backingArray,
                                                   terms,
                                                   n_docs,
                                                   address,
                                                   offsets,
                                                   j);
                if (!hasNext) {
                    break;
                }
            }

            return new LongArrayIntValueLookup(backingArray);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(SimpleIntTermIterator iter,
                                                           int numDocs,
                                                           String field,
                                                           String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cacheFile,
                                               new MMapLongFieldCacherOp());
            }
            return new MMapLongArrayIntValueLookup(buffer, numDocs);
        }

        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapLongFieldCacherOp implements CacheToFileOperation<MMapBuffer> {

            @Override
            public MMapBuffer execute(SimpleIntTermIterator iter,
                                      int numDocs,
                                      File f) throws IOException {
                final int length = numDocs * 8;
                final MMapBuffer buffer = new MMapBuffer(f,
                                                         0L,
                                                         length,
                                                         FileChannel.MapMode.READ_WRITE,
                                                         ByteOrder.LITTLE_ENDIAN);

                long[] terms = new long[BUFFER_SIZE];
                int[] n_docs = new int[BUFFER_SIZE];
                long[] offsets = new long[BUFFER_SIZE];
                long address = iter.getDocListAddress();

                while (true) {
                    int j;
                    boolean hasNext = iter.next();
                    for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                        terms[j] = iter.term();
                        n_docs[j] = iter.docFreq();
                        offsets[j] = iter.getOffset();
                        hasNext = iter.next();
                    }
                    nativeCacheLongMetricValuesMMap(buffer.memory().getAddress(),
                                                    terms,
                                                    n_docs,
                                                    address,
                                                    offsets,
                                                    j);
                    if (!hasNext) {
                        break;
                    }
                }
                buffer.sync(0, length);
                return buffer;
            }
        }
    },
    INT {
        @Override
        public long memoryRequired(int numDocs) {
            return 4L * numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(SimpleIntTermIterator iter,
                                                       int numDocs) throws IOException {
            int[] backingArray = new int[numDocs];

            int[] terms = new int[BUFFER_SIZE];
            int[] n_docs = new int[BUFFER_SIZE];
            long[] offsets = new long[BUFFER_SIZE];
            long address = iter.getDocListAddress();
            while (true) {
                int j;
                boolean hasNext = iter.next();
                for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                    terms[j] = (int) iter.term();
                    n_docs[j] = iter.docFreq();
                    offsets[j] = iter.getOffset();
                    hasNext = iter.next();
                }
                nativeCacheIntMetricValuesInArray(backingArray,
                                                  terms,
                                                  n_docs,
                                                  address,
                                                  offsets,
                                                  j);
                if (!hasNext) {
                    break;
                }
            }

            return new IntArrayIntValueLookup(backingArray);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(SimpleIntTermIterator iter,
                                                           int numDocs,
                                                           String field,
                                                           String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cacheFile,
                                               new MMapIntFieldCacherOp());
            }
            return new MMapIntArrayIntValueLookup(buffer, numDocs);
        }

        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapIntFieldCacherOp implements CacheToFileOperation<MMapBuffer> {

            @Override
            public MMapBuffer execute(SimpleIntTermIterator iter,
                                      int numDocs,
                                      File f) throws IOException {
                final int length = numDocs * 4;
                final MMapBuffer buffer = new MMapBuffer(f,
                                                         0L,
                                                         length,
                                                         FileChannel.MapMode.READ_WRITE,
                                                         ByteOrder.LITTLE_ENDIAN);

                int[] terms = new int[BUFFER_SIZE];
                int[] n_docs = new int[BUFFER_SIZE];
                long[] offsets = new long[BUFFER_SIZE];
                long address = iter.getDocListAddress();
                boolean hasNext = iter.next();
                while (true) {
                    int j;
                    for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                        terms[j] = (int) iter.term();
                        offsets[j] = iter.getOffset();
                        n_docs[j] = iter.docFreq();
                        hasNext = iter.next();
                    }
                    nativeCacheIntMetricValuesMMap(buffer.memory().getAddress(),
                                                   terms,
                                                   n_docs,
                                                   address,
                                                   offsets,
                                                   j);
                    if (!hasNext) {
                        break;
                    }
                }
                buffer.sync(0, length);
                return buffer;
            }
        }
    },
    SHORT {
        @Override
        public long memoryRequired(int numDocs) {
            return 2L * numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(SimpleIntTermIterator iter,
                                                       int numDocs) throws IOException {
            short[] backingArray = new short[numDocs];

            short[] terms = new short[BUFFER_SIZE];
            int[] n_docs = new int[BUFFER_SIZE];
            long[] offsets = new long[BUFFER_SIZE];
            long address = iter.getDocListAddress();
            while (true) {
                int j;
                boolean hasNext = iter.next();
                for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                    terms[j] = (short) iter.term();
                    n_docs[j] = iter.docFreq();
                    offsets[j] = iter.getOffset();
                    hasNext = iter.next();
                }
                nativeCacheShortMetricValuesInArray(backingArray,
                                                    terms,
                                                    n_docs,
                                                    address,
                                                    offsets,
                                                    j);
                if (!hasNext) {
                    break;
                }
            }

            return new ShortArrayIntValueLookup(backingArray);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(SimpleIntTermIterator iter,
                                                           int numDocs,
                                                           String field,
                                                           String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cacheFile,
                                               new MMapShortFieldCacherOp());
            }
            return new MMapShortArrayIntValueLookup(buffer, numDocs);
        }

        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapShortFieldCacherOp implements CacheToFileOperation<MMapBuffer> {

            @Override
            public MMapBuffer execute(SimpleIntTermIterator iter,
                                      int numDocs,
                                      File f) throws IOException {
                final int length = numDocs * 2;
                final MMapBuffer buffer = new MMapBuffer(f,
                                                         0L,
                                                         length,
                                                         FileChannel.MapMode.READ_WRITE,
                                                         ByteOrder.LITTLE_ENDIAN);

                short[] terms = new short[BUFFER_SIZE];
                int[] n_docs = new int[BUFFER_SIZE];
                long[] offsets = new long[BUFFER_SIZE];
                long address = iter.getDocListAddress();
                while (true) {
                    int j;
                    boolean hasNext = iter.next();
                    for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                        terms[j] = (short) iter.term();
                        n_docs[j] = iter.docFreq();
                        offsets[j] = iter.getOffset();
                        hasNext = iter.next();
                    }
                    nativeCacheShortMetricValuesMMap(buffer.memory().getAddress(),
                                                     terms,
                                                     n_docs,
                                                     address,
                                                     offsets,
                                                     j);
                    if (!hasNext) {
                        break;
                    }
                }
                buffer.sync(0, length);
                return buffer;
            }
        }
    },
    CHAR {
        @Override
        public long memoryRequired(int numDocs) {
            return 2L * numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(SimpleIntTermIterator iter,
                                                       int numDocs) throws IOException  {
            char[] backingArray = new char[numDocs];

            char[] terms = new char[BUFFER_SIZE];
            int[] n_docs = new int[BUFFER_SIZE];
            long[] offsets = new long[BUFFER_SIZE];
            long address = iter.getDocListAddress();
            while (true) {
                int j;
                boolean hasNext = iter.next();
                for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                    terms[j] = (char) iter.term();
                    n_docs[j] = iter.docFreq();
                    offsets[j] = iter.getOffset();
                    hasNext = iter.next();
                }
                nativeCacheCharMetricValuesInArray(backingArray,
                                                   terms,
                                                   n_docs,
                                                   address,
                                                   offsets,
                                                   j);
                if (!hasNext) {
                    break;
                }
            }

            return new CharArrayIntValueLookup(backingArray);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(SimpleIntTermIterator iter,
                                                           int numDocs,
                                                           String field,
                                                           String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cacheFile,
                                               new MMapCharFieldCacherOp());
            }
            return new MMapCharArrayIntValueLookup(buffer, numDocs);
        }

        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapCharFieldCacherOp implements CacheToFileOperation<MMapBuffer> {

            @Override
            public MMapBuffer execute(SimpleIntTermIterator iter,
                                      int numDocs,
                                      File f) throws IOException {
                final int length = numDocs * 2;
                final MMapBuffer buffer = new MMapBuffer(f,
                                                         0L,
                                                         length,
                                                         FileChannel.MapMode.READ_WRITE,
                                                         ByteOrder.LITTLE_ENDIAN);

                char[] terms = new char[BUFFER_SIZE];
                int[] n_docs = new int[BUFFER_SIZE];
                long[] offsets = new long[BUFFER_SIZE];
                long address = iter.getDocListAddress();
                while (true) {
                    int j;
                    boolean hasNext = iter.next();
                    for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                        terms[j] = (char) iter.term();
                        n_docs[j] = iter.docFreq();
                        offsets[j] = iter.getOffset();
                        hasNext = iter.next();
                    }
                    nativeCacheCharMetricValuesMMap(buffer.memory().getAddress(),
                                                    terms,
                                                    n_docs,
                                                    address,
                                                    offsets,
                                                    j);
                    if (!hasNext) {
                        break;
                    }
                }
                buffer.sync(0, length);
                return buffer;
            }
        }
    },
    SIGNED_BYTE {
        @Override
        public long memoryRequired(int numDocs) {
            return 1L * numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(SimpleIntTermIterator iter,
                                                       int numDocs) throws IOException {
            byte[] backingArray = new byte[numDocs];

            byte[] terms = new byte[BUFFER_SIZE];
            int[] n_docs = new int[BUFFER_SIZE];
            long[] offsets = new long[BUFFER_SIZE];
            long address = iter.getDocListAddress();
            while (true) {
                int j;
                boolean hasNext = iter.next();
                for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                    terms[j] = (byte) iter.term();
                    n_docs[j] = iter.docFreq();
                    offsets[j] = iter.getOffset();
                    hasNext = iter.next();
                }
                nativeCacheByteMetricValuesInArray(backingArray,
                                                   terms,
                                                   n_docs,
                                                   address,
                                                   offsets,
                                                   j);
                if (!hasNext) {
                    break;
                }
            }

            return new SignedByteArrayIntValueLookup(backingArray);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(SimpleIntTermIterator iter,
                                                           int numDocs,
                                                           String field,
                                                           String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cacheFile,
                                               new MMapByteFieldCacherOp());
            }
            return new MMapSignedByteArrayIntValueLookup(buffer, numDocs);
        }

        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapByteFieldCacherOp implements CacheToFileOperation<MMapBuffer> {

            @Override
            public MMapBuffer execute(SimpleIntTermIterator iter,
                                      int numDocs,
                                      File f) throws IOException {
                final int length = numDocs;
                final MMapBuffer buffer = new MMapBuffer(f,
                                                         0L,
                                                         length,
                                                         FileChannel.MapMode.READ_WRITE,
                                                         ByteOrder.LITTLE_ENDIAN);

                byte[] terms = new byte[BUFFER_SIZE];
                int[] n_docs = new int[BUFFER_SIZE];
                long[] offsets = new long[BUFFER_SIZE];
                long address = iter.getDocListAddress();
                while (true) {
                    int j;
                    boolean hasNext = iter.next();
                    for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                        terms[j] = (byte) iter.term();
                        n_docs[j] = iter.docFreq();
                        offsets[j] = iter.getOffset();
                        hasNext = iter.next();
                    }
                    nativeCacheByteMetricValuesMMap(buffer.memory().getAddress(),
                                                    terms,
                                                    n_docs,
                                                    address,
                                                    offsets,
                                                    j);
                    if (!hasNext) {
                        break;
                    }
                }
                buffer.sync(0, length);
                return buffer;
            }
        }
    },
    BYTE {
        @Override
        public long memoryRequired(int numDocs) {
            return numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(SimpleIntTermIterator iter,
                                                       int numDocs) throws IOException  {
            byte[] backingArray = new byte[numDocs];

            byte[] terms = new byte[BUFFER_SIZE];
            int[] n_docs = new int[BUFFER_SIZE];
            long[] offsets = new long[BUFFER_SIZE];
            long address = iter.getDocListAddress();
            while (true) {
                int j;
                boolean hasNext = iter.next();
                for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                    terms[j] = (byte) iter.term();
                    n_docs[j] = iter.docFreq();
                    offsets[j] = iter.getOffset();
                    hasNext = iter.next();
                }
                nativeCacheByteMetricValuesInArray(backingArray,
                                                   terms,
                                                   n_docs,
                                                   address,
                                                   offsets,
                                                   j);
                if (!hasNext) {
                    break;
                }
            }

            return new ByteArrayIntValueLookup(backingArray);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(SimpleIntTermIterator iter,
                                                           int numDocs,
                                                           String field,
                                                           String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cacheFile,
                                               new MMapByteFieldCacherOp());
            }
            return new MMapByteArrayIntValueLookup(buffer, numDocs);
        }

        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapByteFieldCacherOp implements CacheToFileOperation<MMapBuffer> {

            @Override
            public MMapBuffer execute(SimpleIntTermIterator iter,
                                      int numDocs,
                                      File f) throws IOException {
                final int length = numDocs;
                final MMapBuffer buffer = new MMapBuffer(f,
                                                         0L,
                                                         length,
                                                         FileChannel.MapMode.READ_WRITE,
                                                         ByteOrder.LITTLE_ENDIAN);

                byte[] terms = new byte[BUFFER_SIZE];
                int[] n_docs = new int[BUFFER_SIZE];
                long[] offsets = new long[BUFFER_SIZE];
                long address = iter.getDocListAddress();
                while (true) {
                    int j;
                    boolean hasNext = iter.next();
                    for (j = 0; j < BUFFER_SIZE && hasNext; j++) {
                        terms[j] = (byte) iter.term();
                        n_docs[j] = iter.docFreq();
                        offsets[j] = iter.getOffset();
                        hasNext = iter.next();
                    }
                    nativeCacheByteMetricValuesMMap(buffer.memory().getAddress(),
                                                    terms,
                                                    n_docs,
                                                    address,
                                                    offsets,
                                                    j);
                    if (!hasNext) {
                        break;
                    }
                }
                buffer.sync(0, length);
                return buffer;
            }
        }
    },
    BITSET {
        @Override
        public long memoryRequired(int numDocs) {
            return 8L * (((long) numDocs + 64) >> 6);
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(SimpleIntTermIterator iter,
                                                       int numDocs) throws IOException  {
            FastBitSet bitset = new FastBitSet(numDocs);

            if (numDocs == 0) {
                return new BitSetIntValueLookup(bitset);
            }
            
            int n_docs;
            long offset;
            long address = iter.getDocListAddress();
            if (! iter.next()) {
                return new BitSetIntValueLookup(bitset);
            }
            if (iter.term() != 1) {
                if (! iter.next()) {
                    /* field must be all 0s */
                    return new BitSetIntValueLookup(bitset);
                }
                if (iter.term() != 1) {
                    throw new UnsupportedOperationException(
                            "BitSet fields should only have term  " + "values of 1 and 0.");
                }
            }
            n_docs = iter.docFreq();
            offset = iter.getOffset();
            nativeCacheBitsetMetricValuesInArray(bitset.getBackingArray(), n_docs, address, offset);
            return new BitSetIntValueLookup(bitset);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(SimpleIntTermIterator iter,
                                                           int numDocs,
                                                           String field,
                                                           String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            try {
                return new MMapBitSetIntValueLookup(cacheFile, numDocs);
            } catch (FileNotFoundException e) {
                final MMapFastBitSet bitset;
                bitset = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cacheFile,
                                               new MMapBitsetFieldCacherOp());
                return new MMapBitSetIntValueLookup(bitset);
            }
        }

        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapBitsetFieldCacherOp implements CacheToFileOperation<MMapFastBitSet> {

            @Override
            public MMapFastBitSet execute(SimpleIntTermIterator iter,
                                          int numDocs,
                                          File f) throws IOException {
                final MMapFastBitSet bitset =
                        new MMapFastBitSet(f, numDocs, FileChannel.MapMode.READ_WRITE);

                if (numDocs == 0) {
                    return bitset;
                }
                
                int n_docs;
                long offset;
                long address = iter.getDocListAddress();
                if (! iter.next()) {
                    return bitset;
                }
                if (iter.term() != 1) {
                    if (! iter.next()) {
                        /* field must be all 0s */
                        return bitset;
                    }
                    if (iter.term() != 1) {
                        throw new UnsupportedOperationException(
                                "BitSet fields should only have term  " + "values of 1 and 0.");
                    }
                }
                n_docs = iter.docFreq();
                offset = iter.getOffset();
                nativeCacheBitsetMetricValuesMmap(bitset.getBackingMemory().getAddress(),
                                                  n_docs,
                                                  address,
                                                  offset);
                return bitset;
            }
        }
    };

    private static final Logger log = Logger.getLogger(NativeFlamdexFieldCacher.class);
    private static final int BUFFER_SIZE = 8192;

    public abstract long memoryRequired(int numDocs);

    protected abstract IntValueLookup newFieldCacheInternal(SimpleIntTermIterator iter,
                                                            int numDocs) throws IOException;

    protected abstract IntValueLookup newMMapFieldCacheInternal(SimpleIntTermIterator iter,
                                                                int numDocs,
                                                                String field,
                                                                String directory) throws IOException;

    public IntValueLookup newFieldCache(IntTermIterator iterator,
                                        int numDocs) throws IOException {
        if (!(iterator instanceof SimpleIntTermIterator)) {
            throw new UnsupportedOperationException(
                    "NativeFlamdexFieldCacher only supports SimpleIntTermIterators.  "
                            + "Please use FieldCacher instead.");
        }

        SimpleIntTermIterator iter = (SimpleIntTermIterator) iterator;
        return newFieldCacheInternal(iter, numDocs);
    }

    public IntValueLookup newMMapFieldCache(IntTermIterator iterator,
                                            int numDocs,
                                            String field,
                                            String directory) throws IOException {
        if (!(iterator instanceof SimpleIntTermIterator)) {
            throw new UnsupportedOperationException(
                    "NativeFlamdexFieldCacher only supports SimpleIntTermIterators.  "
                            + "Please use FieldCacher instead.");
        }

        SimpleIntTermIterator iter = (SimpleIntTermIterator) iterator;
        return newMMapFieldCacheInternal(iter, numDocs, field, directory);
    }

    @VisibleForTesting
    private final IntValueLookup newFieldCache(String field, FlamdexReader r) throws IOException {
        final IntTermIterator iterator = r.getIntTermIterator(field);
        try {
            return newFieldCache(iterator, r.getNumDocs());
        } finally {
            iterator.close();
        }
    }

    @VisibleForTesting
    private final IntValueLookup newMMapFieldCache(String field, FlamdexReader r, String directory) throws IOException {
        final IntTermIterator iterator = r.getIntTermIterator(field);
        try {
            return newMMapFieldCache(iterator, r.getNumDocs(), field, directory);
        } finally {
            iterator.close();
        }
    }

    @VisibleForTesting
    abstract String getMMapFileName(String field);

    private static void delete(File f) {
        if (!f.delete()) {
            log.error("unable to delete file " + f);
        }
    }

    private static <T extends Closeable> T cacheToFileAtomically(SimpleIntTermIterator iterator,
                                                                 int numDocs,
                                                                 String field,
                                                                 String directory,
                                                                 File cacheFile,
                                                                 CacheToFileOperation<T> op) throws IOException {
        final File tmp = new File(directory, "fld-" + field + ".intcache." + UUID.randomUUID());
        final T ret;
        try {
            ret = op.execute(iterator, numDocs, tmp);
        } catch (RuntimeException e) {
            delete(tmp);
            throw e;
        } catch (IOException e) {
            delete(tmp);
            throw e;
        }
        if (!tmp.renameTo(cacheFile)) {
            delete(tmp);
            Closeables2.closeQuietly(ret, log);
            throw new IOException("unable to rename " + tmp + " to " + cacheFile);
        }
        return ret;
    }

    private static interface CacheToFileOperation<T> {
        T execute(SimpleIntTermIterator iterator, int numDocs, File f) throws IOException;
    }


    /*
     *
     *
     *  Native function prototypes:
     *
     *
     */
    private native static void nativeCacheBitsetMetricValuesMmap(long save_address,
                                                                 int n_docs,
                                                                 long doc_list_address,
                                                                 long offset);

    private native static void nativeCacheBitsetMetricValuesInArray(long[] backingArray,
                                                                    int n_docs,
                                                                    long doc_list_address,
                                                                    long offset);

    private native static void nativeCacheCharMetricValuesMMap(long save_address,
                                                               char[] terms,
                                                               int[] n_docs,
                                                               long doc_list_address,
                                                               long[] offsets,
                                                               int j);

    private native static void nativeCacheCharMetricValuesInArray(char[] backingArray,
                                                                  char[] terms,
                                                                  int[] n_docs,
                                                                  long doc_list_address,
                                                                  long[] offsets,
                                                                  int j);

    private native static void nativeCacheByteMetricValuesMMap(long save_address,
                                                               byte[] terms,
                                                               int[] n_docs,
                                                               long doc_list_address,
                                                               long[] offsets,
                                                               int j);

    private native static void nativeCacheByteMetricValuesInArray(byte[] backingArray,
                                                                  byte[] terms,
                                                                  int[] n_docs,
                                                                  long doc_list_address,
                                                                  long[] offsets,
                                                                  int j);

    private native static void nativeCacheShortMetricValuesMMap(long save_address,
                                                                short[] terms,
                                                                int[] n_docs,
                                                                long doc_list_address,
                                                                long[] offsets,
                                                                int j);

    private native static void nativeCacheShortMetricValuesInArray(short[] backingArray,
                                                                   short[] terms,
                                                                   int[] n_docs,
                                                                   long doc_list_address,
                                                                   long[] offsets,
                                                                   int j);

    private native static void nativeCacheIntMetricValuesMMap(long save_address,
                                                              int[] terms,
                                                              int[] n_docs,
                                                              long doc_list_address,
                                                              long[] offsets,
                                                              int j);

    private native static void nativeCacheIntMetricValuesInArray(int[] backingArray,
                                                                 int[] terms,
                                                                 int[] n_docs,
                                                                 long doc_list_address,
                                                                 long[] offsets,
                                                                 int j);

    private native static void nativeCacheLongMetricValuesMMap(long save_address,
                                                               long[] terms,
                                                               int[] n_docs,
                                                               long doc_list_address,
                                                               long[] offsets,
                                                               int j);

    private native static void nativeCacheLongMetricValuesInArray(long[] backingArray,
                                                                  long[] terms,
                                                                  int[] n_docs,
                                                                  long doc_list_address,
                                                                  long[] offsets,
                                                                  int j);

}
