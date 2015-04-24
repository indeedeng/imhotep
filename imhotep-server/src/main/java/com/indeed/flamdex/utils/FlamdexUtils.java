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
 package com.indeed.flamdex.utils;

import com.indeed.util.core.threads.ThreadSafeBitSet;
import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.MMapFastBitSet;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIteratorImpl;
import com.indeed.util.io.VIntUtils;
import com.indeed.util.mmap.ByteArray;
import com.indeed.util.mmap.CharArray;
import com.indeed.util.mmap.IntArray;
import com.indeed.util.mmap.LongArray;
import com.indeed.util.mmap.MMapBuffer;
import com.indeed.util.mmap.ShortArray;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * @author jsgroth
 */
public class FlamdexUtils {
    private static final Logger LOG = Logger.getLogger(FlamdexUtils.class);

    private static final int BUFFER_SIZE = 32;

    public static int[] cacheIntField(String field, FlamdexReader reader) {
        final UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field);
        try {
            return cacheIntField(iterator, reader.getNumDocs());
        } finally {
            iterator.close();
        }
    }

    public static long[] cacheLongField(String field, FlamdexReader reader) {
        final UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field);
        try {
            return cacheLongField(iterator, reader.getNumDocs());
        } finally {
            iterator.close();
        }
    }

    public static long[] cacheLongField(UnsortedIntTermDocIterator iterator, int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final long[] cache = new long[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = term;
                }
                if (n < BUFFER_SIZE) break;
            }
        }

        return cache;
    }

    public static MMapBuffer cacheLongFieldToFile(UnsortedIntTermDocIterator iterator, int numDocs, File file) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int length = numDocs * 8;
        final MMapBuffer buffer = new MMapBuffer(file, 0L, length, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN);
        final LongArray longArray = buffer.memory().longArray(0, numDocs);
        try {
            while (iterator.nextTerm()) {
                final long term = iterator.term();
                while (true) {
                    final int n = iterator.nextDocs(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        longArray.set(docIdBuf[i], term);
                    }
                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            buffer.sync(0, length);
        } catch (RuntimeException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        } catch (IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static int[] cacheIntField(UnsortedIntTermDocIterator iterator, int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int[] cache = new int[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = (int)term;
                }
                if (n < BUFFER_SIZE) break;
            }
        }

        return cache;
    }

    public static MMapBuffer cacheIntFieldToFile(UnsortedIntTermDocIterator iterator, int numDocs, File file) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int length = numDocs * 4;
        final MMapBuffer buffer = new MMapBuffer(file, 0L, length, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN);
        final IntArray intArray = buffer.memory().intArray(0, numDocs);
        try {
            while (iterator.nextTerm()) {
                final long term = iterator.term();
                while (true) {
                    final int n = iterator.nextDocs(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        intArray.set(docIdBuf[i], (int)term);
                    }
                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            buffer.sync(0, length);
        } catch (RuntimeException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        } catch (IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static char[] cacheCharField(String field, FlamdexReader reader) {
        final UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field);
        try {
            return cacheCharField(iterator, reader.getNumDocs());
        } finally {
            iterator.close();
        }
    }

    public static char[] cacheCharField(UnsortedIntTermDocIterator iterator, int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final char[] cache = new char[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = (char)term;
                }

                if (n < BUFFER_SIZE) break;
            }
        }

        return cache;
    }

    public static MMapBuffer cacheCharFieldToFile(UnsortedIntTermDocIterator iterator, int numDocs, File file) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int length = numDocs * 2;
        final MMapBuffer buffer = new MMapBuffer(file, 0L, length, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN);
        final CharArray charArray = buffer.memory().charArray(0, numDocs);
        try {
            while (iterator.nextTerm()) {
                final char term = (char)iterator.term();
                while (true) {
                    final int n = iterator.nextDocs(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        charArray.set(docIdBuf[i], term);
                    }
                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            buffer.sync(0, length);
        } catch (RuntimeException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        } catch (IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static short[] cacheShortField(UnsortedIntTermDocIterator iterator, int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final short[] cache = new short[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = (short)term;
                }

                if (n < BUFFER_SIZE) break;
            }
        }

        return cache;
    }

    public static MMapBuffer cacheShortFieldToFile(UnsortedIntTermDocIterator iterator, int numDocs, File file) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int length = numDocs * 2;
        final MMapBuffer buffer = new MMapBuffer(file, 0L, length, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN);
        final ShortArray shortArray = buffer.memory().shortArray(0, numDocs);
        try {
            while (iterator.nextTerm()) {
                final short term = (short)iterator.term();
                while (true) {
                    final int n = iterator.nextDocs(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        shortArray.set(docIdBuf[i], term);
                    }
                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            buffer.sync(0, length);
        } catch (RuntimeException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        } catch (IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static byte[] cacheByteField(String field, FlamdexReader reader) {
        final UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field);
        try {
            return cacheByteField(iterator, reader.getNumDocs());
        } finally {
            iterator.close();
        }
    }

    public static byte[] cacheByteField(UnsortedIntTermDocIterator iterator, int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final byte[] cache = new byte[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = (byte)term;
                }

                if (n < BUFFER_SIZE) break;
            }
        }

        return cache;
    }

    public static MMapBuffer cacheByteFieldToFile(UnsortedIntTermDocIterator iterator, int numDocs, File file) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final MMapBuffer buffer = new MMapBuffer(file, 0L, numDocs, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN);
        final ByteArray byteArray = buffer.memory().byteArray(0, numDocs);
        try {
            while (iterator.nextTerm()) {
                final byte term = (byte)iterator.term();
                while (true) {
                    final int n = iterator.nextDocs(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        byteArray.set(docIdBuf[i], term);
                    }
                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            buffer.sync(0, numDocs);
        } catch (RuntimeException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        } catch (IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static FastBitSet cacheBitSetField(String field, FlamdexReader reader) {
        final UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field);
        try {
            return cacheBitSetField(iterator, reader.getNumDocs());
        } finally {
            iterator.close();
        }
    }

    public static FastBitSet cacheBitSetField(UnsortedIntTermDocIterator iterator, int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final FastBitSet cache = new FastBitSet(numDocs);
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            final boolean boolVal = (term == 1);
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache.set(docIdBuf[i], boolVal);
                }

                if (n < BUFFER_SIZE) break;
            }
        }

        return cache;
    }

    public static MMapFastBitSet cacheBitSetFieldToFile(UnsortedIntTermDocIterator iterator, int numDocs, File file) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final MMapFastBitSet cache = new MMapFastBitSet(file, numDocs, FileChannel.MapMode.READ_WRITE);
        try {
            while (iterator.nextTerm()) {
                final long term = iterator.term();
                final boolean boolVal = (term == 1);
                while (true) {
                    final int n = iterator.nextDocs(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        cache.set(docIdBuf[i], boolVal);
                    }

                    if (n < BUFFER_SIZE) break;
                }
            }
            cache.sync();
        } catch (RuntimeException e) {
            Closeables2.closeQuietly(cache, LOG);
            throw e;
        } catch (IOException e) {
            Closeables2.closeQuietly(cache, LOG);
            throw e;
        }

        return cache;
    }

    public static String[] cacheStringField(String field, FlamdexReader reader) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final String[] cache = new String[reader.getNumDocs()];
        final DocIdStream docIdStream = reader.getDocIdStream();
        try {
            final StringTermIterator it = reader.getStringTermIterator(field);
            try {
                while (it.next()) {
                    docIdStream.reset(it);
                    final String term = it.term();
                    while (true) {
                        final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                        for (int i = 0; i < n; ++i) {
                            cache[docIdBuf[i]] = term;
                        }

                        if (n < BUFFER_SIZE) break;
                    }
                }
            } finally {
                it.close();
            }
        } finally {
            docIdStream.close();
        }

        return cache;
    }

    public static float[] cacheStringFieldAsFloat(String field, FlamdexReader reader, boolean ignoreNonFloats) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final float[] cache = new float[reader.getNumDocs()];
        final DocIdStream docIdStream = reader.getDocIdStream();
        try {
            final StringTermIterator it = reader.getStringTermIterator(field);
            try {
                while (it.next()) {
                    final float term;
                    try {
                        term = Float.parseFloat(it.term());
                    } catch (NumberFormatException e) {
                        if (!ignoreNonFloats) {
                            throw e;
                        }
                        continue;
                    }

                    docIdStream.reset(it);
                    while (true) {
                        final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                        for (int i = 0; i < n; ++i) {
                            cache[docIdBuf[i]] = term;
                        }
                        if (n < BUFFER_SIZE) break;
                    }
                }
            } finally {
                it.close();
            }
        } finally {
            docIdStream.close();
        }

        return cache;
    }

    public static long[] getMinMaxTerm(String field, FlamdexReader r) {
        final IntTermIterator iterator = r.getIntTermIterator(field);
        long minTerm = Long.MAX_VALUE;
        long maxTerm = Long.MIN_VALUE;
        try {
            while (iterator.next()) {
                maxTerm = Math.max(maxTerm, iterator.term());
                minTerm = Math.min(minTerm, iterator.term());
            }
        } finally {
            iterator.close();
        }
        return new long[]{minTerm, maxTerm};
    }

    public static int writeVLong(long i, OutputStream out) throws IOException {
        return VIntUtils.writeVInt64(out, i);
    }

    public static long readVLong(InputStream in) throws IOException {
        long ret = 0L;
        int shift = 0;
        do {
            int b = in.read();
            if (b < 0) {
                //sorry
                if (shift != 0) {
                    throw new IllegalStateException();
                }
                throw new EOFException();
            }
            ret |= ((b & 0x7FL) << shift);
            if (b < 0x80) return ret;
            shift += 7;
        } while (true);
    }

    public static ThreadSafeBitSet cacheHasIntTerm(final String field, final long term, final FlamdexReader reader) {
        final ThreadSafeBitSet ret = new ThreadSafeBitSet(reader.getNumDocs());
        final IntTermIterator iter = reader.getIntTermIterator(field);
        try {
            iter.reset(term);
            if (iter.next() && iter.term() == term) {
                final DocIdStream dis = reader.getDocIdStream();
                dis.reset(iter);
                fillBitSet(dis, ret);
                dis.close();
            }
        } finally {
            iter.close();
        }
        return ret;
    }

    private static void fillBitSet(DocIdStream dis, ThreadSafeBitSet ret) {
        final int[] docIdBuffer = new int[64];
        while (true) {
            final int n = dis.fillDocIdBuffer(docIdBuffer);
            for (int i = 0; i < n; ++i) {
                ret.set(docIdBuffer[i]);
            }
            if (n < docIdBuffer.length) break;
        }
    }

    public static ThreadSafeBitSet cacheHasStringTerm(final String field, final String term, final FlamdexReader reader) {
        final ThreadSafeBitSet ret = new ThreadSafeBitSet(reader.getNumDocs());
        final StringTermIterator iter = reader.getStringTermIterator(field);
        try {
            iter.reset(term);
            if (iter.next() && iter.term().equals(term)) {
                final DocIdStream dis = reader.getDocIdStream();
                dis.reset(iter);
                fillBitSet(dis, ret);
                dis.close();
            }
        } finally {
            iter.close();
        }
        return ret;
    }

    public static ThreadSafeBitSet cacheRegex(final String field, final String regex, final FlamdexReader reader) {
        final Automaton automaton = new RegExp(regex).toAutomaton();
        final ThreadSafeBitSet ret = new ThreadSafeBitSet(reader.getNumDocs());
        if (reader.getIntFields().contains(field)) {
            cacheIntFieldRegex(field, reader, automaton, ret);
        } else if (reader.getStringFields().contains(field)) {
            cacheStringFieldRegex(field, reader, automaton, ret);
        } else {
            // No exception on unknown field because fields can be added and queries can legitimately cross boundaries
            // where the field isn't defined. Instead, just return an empty bitset.
        }
        return ret;
    }

    private static void cacheIntFieldRegex(String field, FlamdexReader reader, Automaton automaton, ThreadSafeBitSet ret) {
        try (final IntTermIterator iter = reader.getIntTermIterator(field);
             final DocIdStream dis = reader.getDocIdStream()) {
            while (iter.next()) {
                if (automaton.run(String.valueOf(iter.term()))) {
                    dis.reset(iter);
                    fillBitSet(dis, ret);
                }
            }
        }
    }

    // TODO: Use automaton.getCommonPrefix() to reset to a start point and short circuit after that prefix?
    private static void cacheStringFieldRegex(String field, FlamdexReader reader, Automaton automaton, ThreadSafeBitSet ret) {
        try (final StringTermIterator iter = reader.getStringTermIterator(field);
             final DocIdStream dis = reader.getDocIdStream()) {
            while (iter.next()) {
                if (automaton.run(iter.term())) {
                    dis.reset(iter);
                    fillBitSet(dis, ret);
                }
            }
        }
    }

    public static long getIntTotalDocFreq(final FlamdexReader r, final String field) {
        final IntTermIterator iter = r.getIntTermIterator(field);
        long totalDocFreq = 0L;
        try {
            while (iter.next()) {
                totalDocFreq += iter.docFreq();
            }
        } finally {
            iter.close();
        }
        return totalDocFreq;
    }

    public static long getStringTotalDocFreq(final FlamdexReader r, final String field) {
        final StringTermIterator iter = r.getStringTermIterator(field);
        long totalDocFreq = 0L;
        try {
            while (iter.next()) {
                totalDocFreq += iter.docFreq();
            }
        } finally {
            iter.close();
        }
        return totalDocFreq;
    }
}
