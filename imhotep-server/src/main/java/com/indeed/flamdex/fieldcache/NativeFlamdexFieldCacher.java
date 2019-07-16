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
package com.indeed.flamdex.fieldcache;

import com.google.common.io.ByteStreams;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.MMapFastBitSet;
import com.indeed.flamdex.simple.SimpleIntTermIterator;
import com.indeed.imhotep.metrics.Constant;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * @author jsgroth
 */
public enum NativeFlamdexFieldCacher {

    LONG {
        class Buffer {
            final long[]   terms = new long[BUFFER_SIZE];
            final int[]   n_docs = new int[BUFFER_SIZE];
            final long[] offsets = new long[BUFFER_SIZE];

            final SimpleIntTermIterator iter;

            Buffer(final SimpleIntTermIterator iter) { this.iter = iter; }

            public long[]   terms() { return terms;   }
            public  int[]  n_docs() { return n_docs;  }
            public long[] offsets() { return offsets; }

            public int fill() {
                int idx = 0;
                while (idx < terms.length && iter.next()) {
                    terms[idx]   = iter.term();
                    n_docs[idx]  = iter.docFreq();
                    offsets[idx] = iter.getOffset();
                    ++idx;
                }
                return idx;
            }
        }

        @Override
        public long memoryRequired(final int numDocs) {
            return 8L * numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(final SimpleIntTermIterator iter,
                                                       final int numDocs, final long min, final long max)
            throws IOException {
            final long[] backingArray = new long[numDocs];
            final long   address      = iter.getDocListAddress();
            final Buffer buffer       = new Buffer(iter);
            int    count        = buffer.fill();
            while (count > 0) {
                nativeCacheLongMetricValuesInArray(backingArray,
                                                   buffer.terms(),
                                                   buffer.n_docs(),
                                                   address,
                                                   buffer.offsets(),
                                                   count);
                count = buffer.fill();
            }
            return new LongArrayIntValueLookup(backingArray, min, max);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(final SimpleIntTermIterator iter,
                                                           final int numDocs,
                                                           final String field,
                                                           final Path directory, final long min, final long max)
            throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new MMapLongFieldCacherOp());
            }
            return new MMapLongArrayIntValueLookup(buffer, numDocs, min, max);
        }

        @Override
        public String getMMapFileName(final String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapLongFieldCacherOp implements CacheToFileOperation<MMapBuffer> {

            @Override
            public MMapBuffer execute(final SimpleIntTermIterator iterator,
                                      final int numDocs,
                                      final Path p) throws IOException {
                final int length = numDocs * 8;
                final MMapBuffer mmapBuffer =
                    new MMapBuffer(p, 0L, length,
                                   FileChannel.MapMode.READ_WRITE,
                                   ByteOrder.LITTLE_ENDIAN);

                final long   address      = iterator.getDocListAddress();
                final Buffer buffer       = new Buffer(iterator);
                int    count        = buffer.fill();
                while (count > 0) {
                    nativeCacheLongMetricValuesMMap(mmapBuffer.memory().getAddress(),
                                                    buffer.terms(),
                                                    buffer.n_docs(),
                                                    address,
                                                    buffer.offsets(),
                                                    count);
                    count = buffer.fill();
                }
                mmapBuffer.sync(0, length);
                return mmapBuffer;
            }
        }
    },
    INT {
        class Buffer {
            final int[]    terms = new int[BUFFER_SIZE];
            final int[]   n_docs = new int[BUFFER_SIZE];
            final long[] offsets = new long[BUFFER_SIZE];

            final SimpleIntTermIterator iter;

            Buffer(final SimpleIntTermIterator iter) { this.iter = iter; }

            public  int[]   terms() { return terms;   }
            public  int[]  n_docs() { return n_docs;  }
            public long[] offsets() { return offsets; }

            public int fill() {
                int idx = 0;
                while (idx < terms.length && iter.next()) {
                    terms[idx]   = (int) iter.term();
                    n_docs[idx]  = iter.docFreq();
                    offsets[idx] = iter.getOffset();
                    ++idx;
                }
                return idx;
            }
        }

        @Override
        public long memoryRequired(final int numDocs) {
            return 4L * numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(final SimpleIntTermIterator iter,
                                                       final int numDocs, final long min, final long max)
            throws IOException {
            final int[] backingArray = new int[numDocs];

            final long address = iter.getDocListAddress();
            final Buffer buffer       = new Buffer(iter);
            int    count        = buffer.fill();
            while (count > 0) {
                nativeCacheIntMetricValuesInArray(backingArray,
                                                  buffer.terms(),
                                                  buffer.n_docs(),
                                                  address,
                                                  buffer.offsets(),
                                                  count);
                count = buffer.fill();
            }
            return new IntArrayIntValueLookup(backingArray, min, max);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(final SimpleIntTermIterator iter,
                                                           final int numDocs,
                                                           final String field,
                                                           final Path directory, final long min, final long max)
            throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new MMapIntFieldCacherOp());
            }
            return new MMapIntArrayIntValueLookup(buffer, numDocs, min, max);
        }

        @Override
        public String getMMapFileName(final String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapIntFieldCacherOp implements CacheToFileOperation<MMapBuffer> {
            @Override
            public MMapBuffer execute(final SimpleIntTermIterator iterator,
                                      final int numDocs,
                                      final Path p) throws IOException {
                final int length = numDocs * 4;
                final MMapBuffer mmapBuffer =
                    new MMapBuffer(p, 0L, length,
                                   FileChannel.MapMode.READ_WRITE,
                                   ByteOrder.LITTLE_ENDIAN);

                final long   address      = iterator.getDocListAddress();
                final Buffer buffer       = new Buffer(iterator);
                int    count        = buffer.fill();
                while (count > 0) {
                    nativeCacheIntMetricValuesMMap(mmapBuffer.memory().getAddress(),
                                                   buffer.terms(),
                                                   buffer.n_docs(),
                                                   address,
                                                   buffer.offsets(),
                                                   count);
                    count = buffer.fill();
                }
                mmapBuffer.sync(0, length);
                return mmapBuffer;
            }
        }
    },
    SHORT {
        class Buffer {
            final short[]   terms = new short[BUFFER_SIZE];
            final int[]    n_docs = new int[BUFFER_SIZE];
            final long[]  offsets = new long[BUFFER_SIZE];

            final SimpleIntTermIterator iter;

            Buffer(final SimpleIntTermIterator iter) { this.iter = iter; }

            public short[]   terms() { return terms;   }
            public   int[]  n_docs() { return n_docs;  }
            public  long[] offsets() { return offsets; }

            public int fill() {
                int idx = 0;
                while (idx < terms.length && iter.next()) {
                    terms[idx]   = (short) iter.term();
                    n_docs[idx]  = iter.docFreq();
                    offsets[idx] = iter.getOffset();
                    ++idx;
                }
                return idx;
            }
        }

        @Override
        public long memoryRequired(final int numDocs) {
            return 2L * numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(final SimpleIntTermIterator iter,
                                                       final int numDocs, final long min, final long max)
            throws IOException {
            final short[] backingArray = new short[numDocs];
            final long   address       = iter.getDocListAddress();
            final Buffer buffer        = new Buffer(iter);
            int    count         = buffer.fill();
            while (count > 0) {
                nativeCacheShortMetricValuesInArray(backingArray,
                                                    buffer.terms(),
                                                    buffer.n_docs(),
                                                    address,
                                                    buffer.offsets(),
                                                    count);
                count = buffer.fill();
            }
            return new ShortArrayIntValueLookup(backingArray, min, max);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(final SimpleIntTermIterator iter,
                                                           final int numDocs,
                                                           final String field,
                                                           final Path directory, final long min, final long max)
            throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new MMapShortFieldCacherOp());
            }
            return new MMapShortArrayIntValueLookup(buffer, numDocs, min, max);
        }

        @Override
        public String getMMapFileName(final String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapShortFieldCacherOp implements CacheToFileOperation<MMapBuffer> {
            @Override
                public MMapBuffer execute(final SimpleIntTermIterator iterator,
                                          final int numDocs,
                                          final Path p) throws IOException {
                final int length = numDocs * 2;
                final MMapBuffer mmapBuffer =
                    new MMapBuffer(p, 0L, length,
                                   FileChannel.MapMode.READ_WRITE,
                                   ByteOrder.LITTLE_ENDIAN);

                final long   address      = iterator.getDocListAddress();
                final Buffer buffer       = new Buffer(iterator);
                int    count        = buffer.fill();
                while (count > 0) {
                    nativeCacheShortMetricValuesMMap(mmapBuffer.memory().getAddress(),
                                                     buffer.terms(),
                                                     buffer.n_docs(),
                                                     address,
                                                     buffer.offsets(),
                                                     count);
                    count = buffer.fill();
                }
                mmapBuffer.sync(0, length);
                return mmapBuffer;
            }
        }
    },
    CHAR {
        class Buffer {
            final char[]   terms = new char[BUFFER_SIZE];
            final int[]   n_docs = new int[BUFFER_SIZE];
            final long[] offsets = new long[BUFFER_SIZE];

            final SimpleIntTermIterator iter;

            Buffer(final SimpleIntTermIterator iter) { this.iter = iter; }

            public char[]    terms() { return terms;   }
            public   int[]  n_docs() { return n_docs;  }
            public  long[] offsets() { return offsets; }

            public int fill() {
                int idx = 0;
                while (idx < terms.length && iter.next()) {
                    terms[idx]   = (char) iter.term();
                    n_docs[idx]  = iter.docFreq();
                    offsets[idx] = iter.getOffset();
                    ++idx;
                }
                return idx;
            }
        }

        @Override
        public long memoryRequired(final int numDocs) {
            return 2L * numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(final SimpleIntTermIterator iter,
                                                       final int numDocs, final long min, final long max)
            throws IOException  {
            final char[] backingArray = new char[numDocs];
            final long   address      = iter.getDocListAddress();
            final Buffer buffer       = new Buffer(iter);
            int    count        = buffer.fill();
            while (count > 0) {
                nativeCacheCharMetricValuesInArray(backingArray,
                                                   buffer.terms(),
                                                   buffer.n_docs(),
                                                   address,
                                                   buffer.offsets(),
                                                   count);
                count = buffer.fill();
            }
            return new CharArrayIntValueLookup(backingArray, min, max);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(final SimpleIntTermIterator iter,
                                                           final int numDocs,
                                                           final String field,
                                                           final Path directory, final long min, final long max)
            throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new MMapCharFieldCacherOp());
            }
            return new MMapCharArrayIntValueLookup(buffer, numDocs, min, max);
        }

        @Override
        public String getMMapFileName(final String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapCharFieldCacherOp implements CacheToFileOperation<MMapBuffer> {
            @Override
                public MMapBuffer execute(final SimpleIntTermIterator iterator,
                                          final int numDocs,
                                          final Path p) throws IOException {
                final int length = numDocs * 2;
                final MMapBuffer mmapBuffer =
                    new MMapBuffer(p, 0L, length,
                                   FileChannel.MapMode.READ_WRITE,
                                   ByteOrder.LITTLE_ENDIAN);

                final long   address      = iterator.getDocListAddress();
                final Buffer buffer       = new Buffer(iterator);
                int    count        = buffer.fill();
                while (count > 0) {
                    nativeCacheCharMetricValuesMMap(mmapBuffer.memory().getAddress(),
                                                    buffer.terms(),
                                                    buffer.n_docs(),
                                                    address,
                                                    buffer.offsets(),
                                                    count);
                    count = buffer.fill();
                }
                mmapBuffer.sync(0, length);
                return mmapBuffer;
            }
        }
    },
    SIGNED_BYTE {
        class Buffer {
            final byte[]   terms = new byte[BUFFER_SIZE];
            final int[]   n_docs = new int[BUFFER_SIZE];
            final long[] offsets = new long[BUFFER_SIZE];

            final SimpleIntTermIterator iter;

            Buffer(final SimpleIntTermIterator iter) { this.iter = iter; }

            public byte[]    terms() { return terms;   }
            public   int[]  n_docs() { return n_docs;  }
            public  long[] offsets() { return offsets; }

            public int fill() {
                int idx = 0;
                while (idx < terms.length && iter.next()) {
                    terms[idx]   = (byte) iter.term();
                    n_docs[idx]  = iter.docFreq();
                    offsets[idx] = iter.getOffset();
                    ++idx;
                }
                return idx;
            }
        }

        @Override
        public long memoryRequired(final int numDocs) {
            return (long) numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(final SimpleIntTermIterator iter,
                                                       final int numDocs, final long min, final long max)
            throws IOException {
            final byte[] backingArray = new byte[numDocs];
            final long   address      = iter.getDocListAddress();
            final Buffer buffer       = new Buffer(iter);
            int    count        = buffer.fill();
            while (count > 0) {
                nativeCacheByteMetricValuesInArray(backingArray,
                                                   buffer.terms(),
                                                   buffer.n_docs(),
                                                   address,
                                                   buffer.offsets(),
                                                   count);
                count = buffer.fill();
            }
            return new SignedByteArrayIntValueLookup(backingArray, min, max);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(final SimpleIntTermIterator iter,
                                                           final int numDocs,
                                                           final String field,
                                                           final Path directory, final long min, final long max)
            throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new MMapByteFieldCacherOp());
            }
            return new MMapSignedByteArrayIntValueLookup(buffer, numDocs, min, max);
        }

        @Override
        public String getMMapFileName(final String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapByteFieldCacherOp implements CacheToFileOperation<MMapBuffer> {
            @Override
                public MMapBuffer execute(final SimpleIntTermIterator iterator,
                                          final int numDocs,
                                          final Path p) throws IOException {
                final int length = numDocs;
                final MMapBuffer mmapBuffer =
                    new MMapBuffer(p, 0L, length,
                                   FileChannel.MapMode.READ_WRITE,
                                   ByteOrder.LITTLE_ENDIAN);

                final long   address      = iterator.getDocListAddress();
                final Buffer buffer       = new Buffer(iterator);
                int    count        = buffer.fill();
                while (count > 0) {
                    nativeCacheByteMetricValuesMMap(mmapBuffer.memory().getAddress(),
                                                    buffer.terms(),
                                                    buffer.n_docs(),
                                                    address,
                                                    buffer.offsets(),
                                                    count);
                    count = buffer.fill();
                }
                mmapBuffer.sync(0, length);
                return mmapBuffer;
            }
        }
    },
    BYTE {
        class Buffer {
            final byte[]   terms = new byte[BUFFER_SIZE];
            final int[]   n_docs = new int[BUFFER_SIZE];
            final long[] offsets = new long[BUFFER_SIZE];

            final SimpleIntTermIterator iter;

            Buffer(final SimpleIntTermIterator iter) { this.iter = iter; }

            public byte[]    terms() { return terms;   }
            public   int[]  n_docs() { return n_docs;  }
            public  long[] offsets() { return offsets; }

            public int fill() {
                int idx = 0;
                while (idx < terms.length && iter.next()) {
                    terms[idx]   = (byte) iter.term();
                    n_docs[idx]  = iter.docFreq();
                    offsets[idx] = iter.getOffset();
                    ++idx;
                }
                return idx;
            }
        }

        @Override
        public long memoryRequired(final int numDocs) {
            return numDocs;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(final SimpleIntTermIterator iter,
                                                       final int numDocs, final long min, final long max)
            throws IOException  {
            final byte[] backingArray = new byte[numDocs];
            final long   address      = iter.getDocListAddress();
            final Buffer buffer       = new Buffer(iter);
            int    count        = buffer.fill();
            while (count > 0) {
                nativeCacheByteMetricValuesInArray(backingArray,
                                                   buffer.terms(),
                                                   buffer.n_docs(),
                                                   address,
                                                   buffer.offsets(),
                                                   count);
                count = buffer.fill();
            }
            return new ByteArrayIntValueLookup(backingArray, min, max);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(final SimpleIntTermIterator iter,
                                                           final int numDocs,
                                                           final String field,
                                                           final Path directory, final long min, final long max)
            throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath,
                                        FileChannel.MapMode.READ_ONLY,
                                        ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new MMapByteFieldCacherOp());
            }
            return new MMapByteArrayIntValueLookup(buffer, numDocs, min, max);
        }

        @Override
        public String getMMapFileName(final String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapByteFieldCacherOp implements CacheToFileOperation<MMapBuffer> {
            @Override
                public MMapBuffer execute(final SimpleIntTermIterator iterator,
                                          final int numDocs,
                                          final Path p) throws IOException {
                final int length = numDocs;
                final MMapBuffer mmapBuffer =
                    new MMapBuffer(p, 0L, length,
                                   FileChannel.MapMode.READ_WRITE,
                                   ByteOrder.LITTLE_ENDIAN);

                final long   address      = iterator.getDocListAddress();
                final Buffer buffer       = new Buffer(iterator);
                int    count        = buffer.fill();
                while (count > 0) {
                    nativeCacheByteMetricValuesMMap(mmapBuffer.memory().getAddress(),
                                                    buffer.terms(),
                                                    buffer.n_docs(),
                                                    address,
                                                    buffer.offsets(),
                                                    count);
                    count = buffer.fill();
                }
                mmapBuffer.sync(0, length);
                return mmapBuffer;
            }
        }
    },
    BITSET {
        @Override
        public long memoryRequired(final int numDocs) {
            return 8L * (((long) numDocs + 64) >> 6);
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(final SimpleIntTermIterator iter,
                                                       final int numDocs, final long min, final long max)
            throws IOException  {
            final FastBitSet bitset = new FastBitSet(numDocs);

            if (numDocs == 0) {
                return new BitSetIntValueLookup(bitset);
            }

            // IntTermIterator could be converted from StringTermIterator so it's possible to have
            // several terms with value 0 or 1 (for example both strings "1" and "001" convert to 1).
            // We iterate through all terms, skipping zeroes and adding ones to bitset.
            while (iter.next()) {
                if (iter.term() == 0) {
                    continue;
                }

                if (iter.term() != 1) {
                    throw new IllegalStateException("Error in NativeFlamdexFieldCacher. Unexpected term " + iter.term());
                }

                final int n_docs = iter.docFreq();
                final long offset = iter.getOffset();
                final long address = iter.getDocListAddress();
                nativeCacheBitsetMetricValuesInArray(bitset.getBackingArray(), n_docs, address, offset);
            }
            return new BitSetIntValueLookup(bitset);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(final SimpleIntTermIterator iter,
                                                           final int numDocs,
                                                           final String field,
                                                           final Path directory, final long min, final long max)
            throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            try {
                return new MMapBitSetIntValueLookup(cachePath, numDocs);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                final MMapFastBitSet bitset;
                bitset = cacheToFileAtomically(iter,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new MMapBitsetFieldCacherOp());
                return new MMapBitSetIntValueLookup(bitset);
            }
        }

        @Override
        public String getMMapFileName(final String field) {
            return "fld-" + field + ".longcache";
        }

        final class MMapBitsetFieldCacherOp implements CacheToFileOperation<MMapFastBitSet> {

            @Override
            public MMapFastBitSet execute(final SimpleIntTermIterator iterator,
                                          final int numDocs,
                                          final Path p) throws IOException {
                final MMapFastBitSet bitset =
                        new MMapFastBitSet(p, numDocs, FileChannel.MapMode.READ_WRITE);

                if (numDocs == 0) {
                    return bitset;
                }

                final int n_docs;
                final long offset;
                final long address = iterator.getDocListAddress();
                if (! iterator.next()) {
                    return bitset;
                }
                if (iterator.term() != 1) {
                    if (! iterator.next()) {
                        /* field must be all 0s */
                        return bitset;
                    }
                    if (iterator.term() != 1) {
                        throw new UnsupportedOperationException(
                                "BitSet fields should only have term  " + "values of 1 and 0.");
                    }
                }
                n_docs = iterator.docFreq();
                offset = iterator.getOffset();
                nativeCacheBitsetMetricValuesMmap(bitset.getBackingMemory().getAddress(),
                                                  n_docs,
                                                  address,
                                                  offset);
                return bitset;
            }
        }
    },
    CONSTANT {
        @Override
        public long memoryRequired(final int numDocs) {
            return 0L;
        }

        @Override
        protected IntValueLookup newFieldCacheInternal(final SimpleIntTermIterator iter,
                                                       final int numDocs, final long min, final long max)
                throws IOException {
            if (min != max) {
                throw new IllegalStateException(
                        "Constant field creation with min=" + min + " and max=" + max);
            }
            return new Constant(min);
        }

        @Override
        protected IntValueLookup newMMapFieldCacheInternal(final SimpleIntTermIterator iter,
                                                           final int numDocs,
                                                           final String field,
                                                           final Path directory, final long min, final long max)
                throws IOException {
            return newFieldCacheInternal(iter, numDocs, min, max);
        }

        @Override
        public String getMMapFileName(final String field) {
            return "fld-" + field + ".constcache";
        }
    };

    private static final Logger log = Logger.getLogger(NativeFlamdexFieldCacher.class);

    static {
        loadNativeLibrary();
        log.info("libfieldcache loaded");
    }

    static void loadNativeLibrary() {
        try {
            final String osName = System.getProperty("os.name");
            final String arch = System.getProperty("os.arch");
            final String resourcePath = "/native/" + osName + "-" + arch + "/libfieldcache.so.1.0.1";
            final File tempFile;
            try (final InputStream is = NativeFlamdexFieldCacher.class.getResourceAsStream(resourcePath)) {
                if (is == null) {
                    throw new FileNotFoundException(
                            "unable to find libfieldcache.so.1.0.1 at resource path " + resourcePath);
                }
                tempFile = File.createTempFile("libfieldcache", ".so");
                try (final OutputStream os = new FileOutputStream(tempFile)) {
                    ByteStreams.copy(is, os);
                }
            }
            System.load(tempFile.getAbsolutePath());
            tempFile.delete();
        } catch (final Throwable e) {
            e.printStackTrace();
            log.warn("unable to load libfieldcache using class loader, looking in java.library.path",
                     e);
            System.loadLibrary("fieldcache"); // if this fails it throws UnsatisfiedLinkError
        }
    }

    private static final int BUFFER_SIZE = 8192;

    public abstract long memoryRequired(int numDocs);

    protected abstract IntValueLookup newFieldCacheInternal(SimpleIntTermIterator iter,
                                                            int numDocs, long min, long max)
    throws IOException;

    protected abstract IntValueLookup newMMapFieldCacheInternal(SimpleIntTermIterator iter,
                                                                int numDocs,
                                                                String field,
                                                                Path directory,
                                                                long min,
                                                                long max) throws IOException;

    public IntValueLookup newFieldCache(final SimpleIntTermIterator iterator, final int numDocs, final long min, final long max)
    throws IOException {
        return newFieldCacheInternal(iterator, numDocs, min, max);
    }

    public IntValueLookup newMMapFieldCache(final SimpleIntTermIterator iterator,
                                            final int numDocs,
                                            final String field,
                                            final Path directory,
                                            final long min,
                                            final long max) throws IOException {
        return newMMapFieldCacheInternal(iterator, numDocs, field, directory, min, max);
    }

    abstract String getMMapFileName(String field);

    private static void deleteQuietly(final Path p) {
        try {
            Files.delete(p);
        } catch (final IOException e){
            log.error("unable to delete file " + p, e);
        }
    }

    private static <T extends Closeable> T cacheToFileAtomically(final SimpleIntTermIterator iterator,
                                                                 final int numDocs,
                                                                 final String field,
                                                                 final Path directory,
                                                                 final Path cachePath,
                                                                 final CacheToFileOperation<T> op)
    throws IOException {
        final Path tmp = directory.resolve("fld-" + field + ".intcache." + UUID.randomUUID());
        try {
            final T ret = op.execute(iterator, numDocs, tmp);
            try {
                Files.move(tmp,
                        cachePath,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (final IOException e) {
                Closeables2.closeQuietly(ret, log);
                throw new IOException("unable to rename " + tmp + " to " + cachePath, e);
            }
            return ret;
        } catch (final Throwable e) {
            deleteQuietly(tmp);
            throw e;
        }
    }

    private interface CacheToFileOperation<T> {
        T execute(SimpleIntTermIterator iterator, int numDocs, Path p) throws IOException;
    }


    /*
     *
     *
     *  Native function prototypes:
     *
     *
     */
    private static native void nativeCacheBitsetMetricValuesMmap(long save_address,
                                                                 int n_docs,
                                                                 long doc_list_address,
                                                                 long offset);

    // Unpacks docIds and sets corresponding bits in a backingArray
    // Bits that were set before method call remain set.
    private static native void nativeCacheBitsetMetricValuesInArray(long[] backingArray,
                                                                    int n_docs,
                                                                    long doc_list_address,
                                                                    long offset);

    private static native void nativeCacheCharMetricValuesMMap(long save_address,
                                                               char[] terms,
                                                               int[] n_docs,
                                                               long doc_list_address,
                                                               long[] offsets,
                                                               int j);

    private static native void nativeCacheCharMetricValuesInArray(char[] backingArray,
                                                                  char[] terms,
                                                                  int[] n_docs,
                                                                  long doc_list_address,
                                                                  long[] offsets,
                                                                  int j);

    private static native void nativeCacheByteMetricValuesMMap(long save_address,
                                                               byte[] terms,
                                                               int[] n_docs,
                                                               long doc_list_address,
                                                               long[] offsets,
                                                               int j);

    private static native void nativeCacheByteMetricValuesInArray(byte[] backingArray,
                                                                  byte[] terms,
                                                                  int[] n_docs,
                                                                  long doc_list_address,
                                                                  long[] offsets,
                                                                  int j);

    private static native void nativeCacheShortMetricValuesMMap(long save_address,
                                                                short[] terms,
                                                                int[] n_docs,
                                                                long doc_list_address,
                                                                long[] offsets,
                                                                int j);

    private static native void nativeCacheShortMetricValuesInArray(short[] backingArray,
                                                                   short[] terms,
                                                                   int[] n_docs,
                                                                   long doc_list_address,
                                                                   long[] offsets,
                                                                   int j);

    private static native void nativeCacheIntMetricValuesMMap(long save_address,
                                                              int[] terms,
                                                              int[] n_docs,
                                                              long doc_list_address,
                                                              long[] offsets,
                                                              int j);

    private static native void nativeCacheIntMetricValuesInArray(int[] backingArray,
                                                                 int[] terms,
                                                                 int[] n_docs,
                                                                 long doc_list_address,
                                                                 long[] offsets,
                                                                 int j);

    private static native void nativeCacheLongMetricValuesMMap(long save_address,
                                                               long[] terms,
                                                               int[] n_docs,
                                                               long doc_list_address,
                                                               long[] offsets,
                                                               int j);

    private static native void nativeCacheLongMetricValuesInArray(long[] backingArray,
                                                                  long[] terms,
                                                                  int[] n_docs,
                                                                  long doc_list_address,
                                                                  long[] offsets,
                                                                  int j);

}
