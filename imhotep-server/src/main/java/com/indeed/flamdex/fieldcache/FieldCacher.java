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
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.MMapFastBitSet;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
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
public enum FieldCacher {
    LONG {
        @Override
        public long memoryRequired(int numDocs) {
            return 8L * numDocs;
        }
        @Override
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator,
                                            int numDocs,
                                            long min,
                                            long max) {
            return new LongArrayIntValueLookup(FlamdexUtils.cacheLongField(iterator, numDocs),
                                               min,
                                               max);
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator,
                                                int numDocs,
                                                String field,
                                                Path directory,
                                                long min, long max) throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new CacheToFileOperation<MMapBuffer>() {
                                                   @Override
                                                   public MMapBuffer execute(
                                                           UnsortedIntTermDocIterator iterator,
                                                           int numDocs,
                                                           Path p) throws IOException {
                                                       return FlamdexUtils.cacheLongFieldToFile(
                                                               iterator,
                                                               numDocs,
                                                               p);
                                                   }
                                               });
            }
            return new MMapLongArrayIntValueLookup(buffer, numDocs, min, max);
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
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator,
                                            int numDocs,
                                            long min,
                                            long max) {
            return new IntArrayIntValueLookup(FlamdexUtils.cacheIntField(iterator, numDocs),
                                              min,
                                              max);
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator,
                                                int numDocs,
                                                String field,
                                                Path directory,
                                                long min, long max) throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new CacheToFileOperation<MMapBuffer>() {
                                                   @Override
                                                   public MMapBuffer execute(
                                                           UnsortedIntTermDocIterator iterator,
                                                           int numDocs,
                                                           Path p) throws IOException {
                                                       return FlamdexUtils.cacheIntFieldToFile(
                                                               iterator,
                                                               numDocs,
                                                               p);
                                                   }
                                               });
            }
            return new MMapIntArrayIntValueLookup(buffer, numDocs, min, max);
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
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator,
                                            int numDocs,
                                            long min,
                                            long max) {
            return new CharArrayIntValueLookup(FlamdexUtils.cacheCharField(iterator, numDocs),
                                               min,
                                               max);
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator,
                                                int numDocs,
                                                String field,
                                                Path directory,
                                                long min, long max) throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new CacheToFileOperation<MMapBuffer>() {
                                                   @Override
                                                   public MMapBuffer execute(
                                                           UnsortedIntTermDocIterator iterator,
                                                           int numDocs,
                                                           Path p) throws IOException {
                                                       return FlamdexUtils.cacheCharFieldToFile(
                                                               iterator,
                                                               numDocs,
                                                               p);
                                                   }
                                               });
            }
            return new MMapCharArrayIntValueLookup(buffer, numDocs, min, max);
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
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator,
                                            int numDocs,
                                            long min,
                                            long max) {
            return new ShortArrayIntValueLookup(FlamdexUtils.cacheShortField(iterator, numDocs),
                                                min,
                                                max);
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator,
                                                int numDocs,
                                                String field,
                                                Path directory,
                                                long min, long max) throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new CacheToFileOperation<MMapBuffer>() {
                                                   @Override
                                                   public MMapBuffer execute(
                                                           UnsortedIntTermDocIterator iterator,
                                                           int numDocs,
                                                           Path p) throws IOException {
                                                       return FlamdexUtils.cacheShortFieldToFile(
                                                               iterator,
                                                               numDocs,
                                                               p);
                                                   }
                                               });
            }
            return new MMapShortArrayIntValueLookup(buffer, numDocs, min, max);
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
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator,
                                            int numDocs,
                                            long min,
                                            long max) {
            return new ByteArrayIntValueLookup(FlamdexUtils.cacheByteField(iterator, numDocs),
                                               min,
                                               max);
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator,
                                                int numDocs,
                                                String field,
                                                Path directory,
                                                long min, long max) throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new CacheToFileOperation<MMapBuffer>() {
                                                   @Override
                                                   public MMapBuffer execute(
                                                           UnsortedIntTermDocIterator iterator,
                                                           int numDocs,
                                                           Path p) throws IOException {
                                                       return FlamdexUtils.cacheByteFieldToFile(
                                                               iterator,
                                                               numDocs,
                                                               p);
                                                   }
                                               });
            }
            return new MMapByteArrayIntValueLookup(buffer, numDocs, min, max);
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
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator,
                                            int numDocs,
                                            long min,
                                            long max) {
            return new SignedByteArrayIntValueLookup(FlamdexUtils.cacheByteField(iterator, numDocs),
                                                     min,
                                                     max);
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator,
                                                int numDocs,
                                                String field,
                                                Path directory,
                                                long min, long max) throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            MMapBuffer buffer;
            try {
                buffer = new MMapBuffer(cachePath, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                buffer = cacheToFileAtomically(iterator,
                                               numDocs,
                                               field,
                                               directory,
                                               cachePath,
                                               new CacheToFileOperation<MMapBuffer>() {
                                                   @Override
                                                   public MMapBuffer execute(
                                                           UnsortedIntTermDocIterator iterator,
                                                           int numDocs,
                                                           Path p) throws IOException {
                                                       return FlamdexUtils.cacheByteFieldToFile(
                                                               iterator,
                                                               numDocs,
                                                               p);
                                                   }
                                               });
            }
            return new MMapSignedByteArrayIntValueLookup(buffer, numDocs, min, max);
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
        public IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator,
                                            int numDocs,
                                            long min,
                                            long max) {
            return new BitSetIntValueLookup(FlamdexUtils.cacheBitSetField(iterator, numDocs));
        }
        @Override
        public IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator,
                                                int numDocs,
                                                String field,
                                                Path directory,
                                                long min, long max) throws IOException {
            final Path cachePath = directory.resolve(getMMapFileName(field));
            try {
                return new MMapBitSetIntValueLookup(cachePath, numDocs);
            } catch (final NoSuchFileException|FileNotFoundException e) {
                // ignore
            }
            final MMapFastBitSet bitSet = cacheToFileAtomically(iterator,
                                                                numDocs,
                                                                field,
                                                                directory,
                                                                cachePath,
                                                                new CacheToFileOperation<MMapFastBitSet>() {
                                                                    @Override
                                                                    public MMapFastBitSet execute(
                                                                            UnsortedIntTermDocIterator iterator,
                                                                            int numDocs,
                                                                            Path p) throws IOException {
                                                                        return FlamdexUtils.cacheBitSetFieldToFile(
                                                                                iterator,
                                                                                numDocs,
                                                                                p);
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

    public final IntValueLookup newFieldCache(String field, FlamdexReader r, long min, long max) {
        final UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(r, field);
        try {
            return newFieldCache(iterator, r.getNumDocs(), min, max);
        } finally {
            iterator.close();
        }
    }

    public abstract IntValueLookup newFieldCache(UnsortedIntTermDocIterator iterator,
                                                 int numDocs,
                                                 long min,
                                                 long max);

    public final IntValueLookup newMMapFieldCache(String field,
                                                  FlamdexReader r,
                                                  Path directory,
                                                  long min,
                                                  long max) throws IOException {
        final UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(r, field);
        try {
            return newMMapFieldCache(iterator, r.getNumDocs(), field, directory, min, max);
        } finally {
            iterator.close();
        }
    }

    public abstract IntValueLookup newMMapFieldCache(UnsortedIntTermDocIterator iterator,
                                                     int numDocs,
                                                     String field,
                                                     Path directory,
                                                     long min,
                                                     long max) throws IOException;

    @VisibleForTesting
    abstract String getMMapFileName(String field);

    private static void delete(Path p) {
        try {
            Files.delete(p);
        } catch (IOException e) {
            log.error("unable to delete file " + p);
        }
    }

    private static <T extends Closeable> T cacheToFileAtomically(UnsortedIntTermDocIterator iterator,
                                                         int numDocs,
                                                         String field,
                                                         Path directory,
                                                         Path cachePath,
                                                         CacheToFileOperation<T> op) throws IOException {
        final Path tmp = directory.resolve("fld-" + field + ".intcache." + UUID.randomUUID());
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
        try {
            Files.move(tmp,
                       cachePath,
                       StandardCopyOption.ATOMIC_MOVE,
                       StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e){
            delete(tmp);
            Closeables2.closeQuietly(ret, log);
            throw new IOException("unable to rename " + tmp + " to " + cachePath, e);
        }
        return ret;
    }

    private static interface CacheToFileOperation<T> {
        T execute(UnsortedIntTermDocIterator iterator, int numDocs, Path p) throws IOException;
    }
}
