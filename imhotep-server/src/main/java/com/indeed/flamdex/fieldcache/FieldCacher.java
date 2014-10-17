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
import com.google.common.base.Charsets;
import com.google.common.io.Closer;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.indeed.util.core.Pair;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.datastruct.MMapFastBitSet;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.util.mmap.BufferResource;
import com.indeed.util.mmap.IntArray;
import com.indeed.util.mmap.MMapBuffer;
import com.indeed.util.mmap.NativeBuffer;
import com.indeed.util.mmap.ZeroCopyOutputStream;

import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.UUID;

/**
 * @author jsgroth
 */
public enum FieldCacher {
    LONG {
        @Override
        public long memoryRequired(int numDocs) {
            return 8L * numDocs;
        }
        @Override
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator, int numDocs) {
            return new LongArrayIntValueLookup(FlamdexUtils.cacheLongField(iterator, numDocs));
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator, int numDocs, String field, String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator, numDocs, field, directory, cacheFile, new CacheToFileOperation<MMapBuffer>() {
                    @Override
                    public MMapBuffer execute(UnsortedIntTermDocIterator iterator, int numDocs, File f) throws IOException {
                        return FlamdexUtils.cacheLongFieldToFile(iterator, numDocs, f);
                    }
                });
            }
            return new MMapLongArrayIntValueLookup(buffer, numDocs);
        }
        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".longcache";
        }
    },
    INT {
        @Override
        public long memoryRequired(int numDocs) {
            return 4L * numDocs;
        }
        @Override
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator, int numDocs) {
            return new IntArrayIntValueLookup(FlamdexUtils.cacheIntField(iterator, numDocs));
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator, int numDocs, String field, String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator, numDocs, field, directory, cacheFile, new CacheToFileOperation<MMapBuffer>() {
                    @Override
                    public MMapBuffer execute(UnsortedIntTermDocIterator iterator, int numDocs, File f) throws IOException {
                        return FlamdexUtils.cacheIntFieldToFile(iterator, numDocs, f);
                    }
                });
            }
            return new MMapIntArrayIntValueLookup(buffer, numDocs);
        }
        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".intcache";
        }
    },
    CHAR {
        @Override
        public long memoryRequired(int numDocs) {
            return 2L * numDocs;
        }
        @Override
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator, int numDocs) {
            return new CharArrayIntValueLookup(FlamdexUtils.cacheCharField(iterator, numDocs));
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator, int numDocs, String field, String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator, numDocs, field, directory, cacheFile, new CacheToFileOperation<MMapBuffer>() {
                    @Override
                    public MMapBuffer execute(UnsortedIntTermDocIterator iterator, int numDocs, File f) throws IOException {
                        return FlamdexUtils.cacheCharFieldToFile(iterator, numDocs, f);
                    }
                });
            }
            return new MMapCharArrayIntValueLookup(buffer, numDocs);
        }
        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".charcache";
        }
    },
    SHORT {
        @Override
        public long memoryRequired(int numDocs) {
            return 2L * numDocs;
        }
        @Override
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator, int numDocs) {
            return new ShortArrayIntValueLookup(FlamdexUtils.cacheShortField(iterator, numDocs));
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator, int numDocs, String field, String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator, numDocs, field, directory, cacheFile, new CacheToFileOperation<MMapBuffer>() {
                    @Override
                    public MMapBuffer execute(UnsortedIntTermDocIterator iterator, int numDocs, File f) throws IOException {
                        return FlamdexUtils.cacheShortFieldToFile(iterator, numDocs, f);
                    }
                });
            }
            return new MMapShortArrayIntValueLookup(buffer, numDocs);
        }
        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".shortcache";
        }
    },
    BYTE {
        @Override
        public long memoryRequired(int numDocs) {
            return numDocs;
        }
        @Override
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator, int numDocs) {
            return new ByteArrayIntValueLookup(FlamdexUtils.cacheByteField(iterator, numDocs));
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator, int numDocs, String field, String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator, numDocs, field, directory, cacheFile, new CacheToFileOperation<MMapBuffer>() {
                    @Override
                    public MMapBuffer execute(UnsortedIntTermDocIterator iterator, int numDocs, File f) throws IOException {
                        return FlamdexUtils.cacheByteFieldToFile(iterator, numDocs, f);
                    }
                });
            }
            return new MMapByteArrayIntValueLookup(buffer, numDocs);
        }
        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".bytecache";
        }
    },
    SIGNED_BYTE {
        @Override
        public long memoryRequired(int numDocs) {
            return numDocs;
        }
        @Override
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator, int numDocs) {
            return new SignedByteArrayIntValueLookup(FlamdexUtils.cacheByteField(iterator, numDocs));
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator, int numDocs, String field, String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cacheFile, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator, numDocs, field, directory, cacheFile, new CacheToFileOperation<MMapBuffer>() {
                    @Override
                    public MMapBuffer execute(UnsortedIntTermDocIterator iterator, int numDocs, File f) throws IOException {
                        return FlamdexUtils.cacheByteFieldToFile(iterator, numDocs, f);
                    }
                });
            }
            return new MMapSignedByteArrayIntValueLookup(buffer, numDocs);
        }
        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".sbytecache";
        }
    },
    BITSET {
        @Override
        public long memoryRequired(int numDocs) {
            return 8L * (((long)numDocs + 64) >> 6);
        }
        @Override
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator, int numDocs) {
            return new BitSetIntValueLookup(FlamdexUtils.cacheBitSetField(iterator, numDocs));
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator, int numDocs, String field, String directory) throws IOException {
            final File cacheFile = new File(directory, getMMapFileName(field));
            try {
                return new MMapBitSetIntValueLookup(cacheFile, numDocs);
            } catch (FileNotFoundException e) {
                // ignore
            }
            final MMapFastBitSet bitSet = cacheToFileAtomically(iterator, numDocs, field, directory, cacheFile, new CacheToFileOperation<MMapFastBitSet>() {
                @Override
                public MMapFastBitSet execute(UnsortedIntTermDocIterator iterator, int numDocs, File f) throws IOException {
                    return FlamdexUtils.cacheBitSetFieldToFile(iterator, numDocs, f);
                }
            });
            return new MMapBitSetIntValueLookup(bitSet);
        }
        @Override
        public String getMMapFileName(String field) {
            return "fld-" + field + ".bitsetcache";
        }
    };

    private static final Logger log = Logger.getLogger(FieldCacher.class);

    public abstract long memoryRequired(int numDocs);

    public final IntValueLookup newFieldCache(String field, FlamdexReader r) {
        final UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(r, field);
        try {
            return newFieldCache(iterator, r.getNumDocs());
        } finally {
            iterator.close();
        }
    }

    public abstract IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator, int numDocs);

    public final IntValueLookup newMMapFieldCache(String field, FlamdexReader r, String directory) throws IOException {
        final UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(r, field);
        try {
            return newMMapFieldCache(iterator, r.getNumDocs(), field, directory);
        } finally {
            iterator.close();
        }
    }

    public static StringValueLookup newStringValueLookup(String field, FlamdexReader r, String directory) throws IOException {
        final Pair<? extends BufferResource, ? extends BufferResource> pair = buildStringValueLookup(field, r, directory);
        return new MMapStringValueLookup(pair.getFirst(), pair.getSecond());
    }

    private static Pair<? extends BufferResource, ? extends BufferResource> buildStringValueLookup(final String field,
                                                                                                   final FlamdexReader r,
                                                                                                   final String directory) throws IOException {
        final Closer closer = Closer.create();
        StringTermDocIterator stringTermDocIterator = null;
        
        try {
            final NativeBuffer offsets;

            offsets = closer.register(new NativeBuffer(4*r.getNumDocs(), ByteOrder.LITTLE_ENDIAN));
            final IntArray intArray = offsets.memory().intArray(0, r.getNumDocs());
            final ZeroCopyOutputStream valuesFileOut = new ZeroCopyOutputStream();
            final CountingOutputStream counter = new CountingOutputStream(new BufferedOutputStream(valuesFileOut));
            final LittleEndianDataOutputStream valuesOut = closer.register(new LittleEndianDataOutputStream(counter));
            valuesOut.writeByte(0);
            stringTermDocIterator = closer.register(r.getStringTermDocIterator(field));
            final int[] docIdBuffer = new int[1024];
            while (stringTermDocIterator.nextTerm()) {
                final int offset = (int) counter.getCount();
                final String term = stringTermDocIterator.term();
                final byte[] bytes = term.getBytes(Charsets.UTF_8);
                if (bytes.length < 0xFF) {
                    valuesOut.writeByte(bytes.length);
                } else {
                    valuesOut.writeByte(0xFF);
                    valuesOut.writeInt(bytes.length);
                }
                valuesOut.write(bytes);
                while (true) {
                    final int n = stringTermDocIterator.fillDocIdBuffer(docIdBuffer);
                    for (int i = 0; i < n; i++) {
                        intArray.set(docIdBuffer[i], offset);
                    }
                    if (n < docIdBuffer.length) break;
                }
            }
            valuesOut.flush();
            final NativeBuffer buffer = valuesFileOut.getBuffer().realloc(valuesFileOut.position());
            return Pair.of(offsets, buffer);
        } catch (Throwable t) {
            closer.close();
            throw Throwables2.propagate(t, IOException.class);
        } finally {
            Closeables2.closeQuietly(stringTermDocIterator, log);
        }
    }

    public abstract IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator, int numDocs, String field, String directory) throws IOException;

    @VisibleForTesting
    abstract String getMMapFileName(String field);

    public static FieldCacher getCacherForField(String field, FlamdexReader r) {
        final long[] minMaxTerm = FlamdexUtils.getMinMaxTerm(field, r);
        final long minTermVal = minMaxTerm[0];
        final long maxTermVal = minMaxTerm[1];

        if (minTermVal >= 0 && maxTermVal <= 1) {
            return BITSET;
        } else if (minTermVal >= 0 && maxTermVal <= 255) {
            return BYTE;
        } else if (minTermVal >= Byte.MIN_VALUE && maxTermVal <= Byte.MAX_VALUE) {
            return SIGNED_BYTE;
        } else if (minTermVal >= 0 && maxTermVal <= 65535) {
            return CHAR;
        } else if (minTermVal >= Short.MIN_VALUE && maxTermVal <= Short.MAX_VALUE) {
            return SHORT;
        } else if (minTermVal >= Integer.MIN_VALUE && maxTermVal <= Integer.MAX_VALUE) {
            return INT;
        } else {
            return LONG;
        }
    }

    private static void delete(File f) {
        if (!f.delete()) {
            log.error("unable to delete file " + f);
        }
    }

    private static <T extends Closeable> T cacheToFileAtomically(UnsortedIntTermDocIterator iterator, int numDocs, String field, String directory, File cacheFile, CacheToFileOperation<T> op) throws IOException {
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
        T execute(UnsortedIntTermDocIterator iterator, int numDocs, File f) throws IOException;
    }
}
