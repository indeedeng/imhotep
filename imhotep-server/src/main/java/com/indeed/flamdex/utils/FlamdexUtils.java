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

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FieldsCardinalityMetadata;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.MMapFastBitSet;
import com.indeed.flamdex.fieldcache.LongArrayIntValueLookup;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIteratorImpl;
import com.indeed.imhotep.automaton.Automaton;
import com.indeed.imhotep.automaton.RegExp;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.threads.ThreadSafeBitSet;
import com.indeed.util.io.VIntUtils;
import com.indeed.util.mmap.ByteArray;
import com.indeed.util.mmap.CharArray;
import com.indeed.util.mmap.IntArray;
import com.indeed.util.mmap.LongArray;
import com.indeed.util.mmap.MMapBuffer;
import com.indeed.util.mmap.ShortArray;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jsgroth
 */
public class FlamdexUtils {
    private FlamdexUtils() {
    }

    private static final Logger LOG = Logger.getLogger(FlamdexUtils.class);

    private static final int BUFFER_SIZE = 32;

    public static int[] cacheIntField(final String field, final FlamdexReader reader) {
        try (UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field)) {
            return cacheIntField(iterator, reader.getNumDocs());
        }
    }

    public static long[] cacheLongField(final String field, final FlamdexReader reader) {
        try (UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field)) {
            return cacheLongField(iterator, reader.getNumDocs());
        }
    }

    public static long[] cacheLongField(final UnsortedIntTermDocIterator iterator, final int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final long[] cache = new long[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = term;
                }
                if (n < BUFFER_SIZE) {
                    break;
                }
            }
        }

        return cache;
    }

    public static long getConstantField(final UnsortedIntTermDocIterator iterator, final int numDocs) {
        if (!iterator.nextTerm()) {
            // if there is no term then all docs have metric value 0
            return 0;
        }
        final long term = iterator.term();
        if (!iterator.nextTerm() && (iterator.docFreq() == numDocs)) {
            // only one term and all docs has this term.
            return term;
        }

        throw new IllegalStateException("Iterator expected to be constant, i.e. zero or one term and all docs have this term");
    }

    public static long getConstantField(final IntTermIterator iterator, final int numDocs) {
        if (!iterator.next()) {
            // if there is no term then all docs have metric value 0
            return 0;
        }
        final long term = iterator.term();
        if (!iterator.next() && (iterator.docFreq() == numDocs)) {
            // only one term and all docs has this term.
            return term;
        }

        throw new IllegalStateException("Iterator expected to be constant, i.e. zero or one term and all docs have this term");
    }

    public static MMapBuffer cacheConstantFieldToFile(final long term,
                                                  final int numDocs,
                                                  final Path path) throws IOException {
        final int length = 8;
        final MMapBuffer buffer = new MMapBuffer(path, 0L, length, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN);
        final LongArray longArray = buffer.memory().longArray(0, 1);
        try {
            longArray.set(0, term);
            buffer.sync(0, length);
        } catch (final RuntimeException | IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static MMapBuffer cacheLongFieldToFile(final UnsortedIntTermDocIterator iterator,
                                                  final int numDocs,
                                                  final Path path) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int length = numDocs * 8;
        final MMapBuffer buffer = new MMapBuffer(path, 0L, length, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN);
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
        } catch (final RuntimeException | IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static int[] cacheIntField(final UnsortedIntTermDocIterator iterator, final int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int[] cache = new int[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = (int)term;
                }
                if (n < BUFFER_SIZE) {
                    break;
                }
            }
        }

        return cache;
    }

    public static MMapBuffer cacheIntFieldToFile(final UnsortedIntTermDocIterator iterator,
                                                 final int numDocs,
                                                 final Path path) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int length = numDocs * 4;
        final MMapBuffer buffer = new MMapBuffer(path,
                                                 0L,
                                                 length,
                                                 FileChannel.MapMode.READ_WRITE,
                                                 ByteOrder.LITTLE_ENDIAN);
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
        } catch (final RuntimeException | IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static char[] cacheCharField(final String field, final FlamdexReader reader) {
        try (UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field)) {
            return cacheCharField(iterator, reader.getNumDocs());
        }
    }

    public static char[] cacheCharField(final UnsortedIntTermDocIterator iterator, final int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final char[] cache = new char[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = (char)term;
                }

                if (n < BUFFER_SIZE) {
                    break;
                }
            }
        }

        return cache;
    }

    public static MMapBuffer cacheCharFieldToFile(final UnsortedIntTermDocIterator iterator,
                                                  final int numDocs,
                                                  final Path path) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int length = numDocs * 2;
        final MMapBuffer buffer = new MMapBuffer(path, 0L, length, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN);
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
        } catch (final RuntimeException | IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static short[] cacheShortField(final UnsortedIntTermDocIterator iterator, final int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final short[] cache = new short[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = (short)term;
                }

                if (n < BUFFER_SIZE) {
                    break;
                }
            }
        }

        return cache;
    }

    public static MMapBuffer cacheShortFieldToFile(final UnsortedIntTermDocIterator iterator,
                                                   final int numDocs,
                                                   final Path path) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final int length = numDocs * 2;
        final MMapBuffer buffer = new MMapBuffer(path, 0L, length, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN);
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
        } catch (final RuntimeException | IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static byte[] cacheByteField(final String field, final FlamdexReader reader) {
        try (UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field)) {
            return cacheByteField(iterator, reader.getNumDocs());
        }
    }

    public static byte[] cacheByteField(final UnsortedIntTermDocIterator iterator, final int numDocs) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final byte[] cache = new byte[numDocs];
        while (iterator.nextTerm()) {
            final long term = iterator.term();
            while (true) {
                final int n = iterator.nextDocs(docIdBuf);
                for (int i = 0; i < n; ++i) {
                    cache[docIdBuf[i]] = (byte)term;
                }

                if (n < BUFFER_SIZE) {
                    break;
                }
            }
        }

        return cache;
    }

    public static MMapBuffer cacheByteFieldToFile(final UnsortedIntTermDocIterator iterator,
                                                  final int numDocs,
                                                  final Path path) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final MMapBuffer buffer = new MMapBuffer(path,
                                                 0L,
                                                 numDocs,
                                                 FileChannel.MapMode.READ_WRITE,
                                                 ByteOrder.LITTLE_ENDIAN);
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
        } catch (final RuntimeException | IOException e) {
            Closeables2.closeQuietly(buffer, LOG);
            throw e;
        }

        return buffer;
    }

    public static FastBitSet cacheBitSetField(final String field, final FlamdexReader reader) {
        try (UnsortedIntTermDocIterator iterator = UnsortedIntTermDocIteratorImpl.create(reader, field)) {
            return cacheBitSetField(iterator, reader.getNumDocs());
        }
    }

    public static FastBitSet cacheBitSetField(final UnsortedIntTermDocIterator iterator, final int numDocs) {
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

                if (n < BUFFER_SIZE) {
                    break;
                }
            }
        }

        return cache;
    }

    public static MMapFastBitSet cacheBitSetFieldToFile(final UnsortedIntTermDocIterator iterator,
                                                        final int numDocs,
                                                        final Path path) throws IOException {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final MMapFastBitSet cache = new MMapFastBitSet(path, numDocs, FileChannel.MapMode.READ_WRITE);
        try {
            while (iterator.nextTerm()) {
                final long term = iterator.term();
                final boolean boolVal = (term == 1);
                while (true) {
                    final int n = iterator.nextDocs(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        cache.set(docIdBuf[i], boolVal);
                    }

                    if (n < BUFFER_SIZE) {
                        break;
                    }
                }
            }
            cache.sync();
        } catch (final RuntimeException | IOException e) {
            Closeables2.closeQuietly(cache, LOG);
            throw e;
        }

        return cache;
    }

    public static String[] cacheStringField(final String field, final FlamdexReader reader) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final String[] cache = new String[reader.getNumDocs()];
        try (DocIdStream docIdStream = reader.getDocIdStream()) {
            try (StringTermIterator it = reader.getStringTermIterator(field)) {
                while (it.next()) {
                    docIdStream.reset(it);
                    final String term = it.term();
                    while (true) {
                        final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                        for (int i = 0; i < n; ++i) {
                            cache[docIdBuf[i]] = term;
                        }

                        if (n < BUFFER_SIZE) {
                            break;
                        }
                    }
                }
            }
        }

        return cache;
    }

    public static float[] cacheStringFieldAsFloat(final String field, final FlamdexReader reader, final boolean ignoreNonFloats) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        final float[] cache = new float[reader.getNumDocs()];
        try (DocIdStream docIdStream = reader.getDocIdStream()) {
            try (StringTermIterator it = reader.getStringTermIterator(field)) {
                while (it.next()) {
                    final float term;
                    try {
                        term = Float.parseFloat(it.term());
                    } catch (final NumberFormatException e) {
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
                        if (n < BUFFER_SIZE) {
                            break;
                        }
                    }
                }
            }
        }

        return cache;
    }

    public static long[] getMinMaxTerm(final String field, final FlamdexReader r) {
        /*
         * Docs with no term are defined to have a term of 0,
         * so the min - max range MUST include 0.
         */
        long minTerm = 0;
        long maxTerm = 0;
        long termsCount = 0;
        long lastTerm = 0;
        long lastTermFreq = 0;
        try (IntTermIterator iterator = r.getUnsortedIntTermIterator(field)) {
            while (iterator.next()) {
                lastTerm = iterator.term();
                maxTerm = Math.max(maxTerm, lastTerm);
                minTerm = Math.min(minTerm, lastTerm);
                termsCount++;
                lastTermFreq = iterator.docFreq();
            }
        }

        // check if all docs have same value
        if ((termsCount == 1) && (lastTermFreq == r.getNumDocs())) {
            return new long[]{lastTerm, lastTerm};
        }

        return new long[]{minTerm, maxTerm};
    }

    public static int writeVLong(final long i, final OutputStream out) throws IOException {
        return VIntUtils.writeVInt64(out, i);
    }

    public static long readVLong(final InputStream in) throws IOException {
        long ret = 0L;
        int shift = 0;
        do {
            final int b = in.read();
            if (b < 0) {
                //sorry
                if (shift != 0) {
                    throw new IllegalStateException();
                }
                throw new EOFException();
            }
            ret |= ((b & 0x7FL) << shift);
            if (b < 0x80) {
                return ret;
            }
            shift += 7;
        } while (true);
    }

    public static ThreadSafeBitSet cacheHasIntTerm(final String field, final long term, final FlamdexReader reader) {
        final ThreadSafeBitSet ret = new ThreadSafeBitSet(reader.getNumDocs());
        try (IntTermIterator iter = reader.getUnsortedIntTermIterator(field)) {
            iter.reset(term);
            if (iter.next() && iter.term() == term) {
                final DocIdStream dis = reader.getDocIdStream();
                dis.reset(iter);
                fillBitSet(dis, ret);
                dis.close();
            }
        }
        return ret;
    }

    private static void fillBitSet(final DocIdStream dis, final ThreadSafeBitSet ret) {
        final int[] docIdBuffer = new int[64];
        while (true) {
            final int n = dis.fillDocIdBuffer(docIdBuffer);
            for (int i = 0; i < n; ++i) {
                ret.set(docIdBuffer[i]);
            }
            if (n < docIdBuffer.length) {
                break;
            }
        }
    }

    public static ThreadSafeBitSet cacheHasStringTerm(final String field, final String term, final FlamdexReader reader) {
        final ThreadSafeBitSet ret = new ThreadSafeBitSet(reader.getNumDocs());
        try (StringTermIterator iter = reader.getStringTermIterator(field)) {
            iter.reset(term);
            if (iter.next() && iter.term().equals(term)) {
                final DocIdStream dis = reader.getDocIdStream();
                dis.reset(iter);
                fillBitSet(dis, ret);
                dis.close();
            }
        }
        return ret;
    }

    public static ThreadSafeBitSet cacheHasIntField(final String field, final FlamdexReader reader) {
        final ThreadSafeBitSet ret = new ThreadSafeBitSet(reader.getNumDocs());
        final int[] docIdBuffer = new int[64]; // 64 instead of BUFFER_SIZE to be consistent with fillBitSet.
        try (
                final IntTermIterator iter = reader.getUnsortedIntTermIterator(field);
                final DocIdStream dis = reader.getDocIdStream()
        ) {
            while (iter.next()) {
                dis.reset(iter);
                fillBitSetUsingBuffer(dis, ret, docIdBuffer);
            }
        }
        return ret;
    }

    public static ThreadSafeBitSet cacheHasStringField(final String field, final FlamdexReader reader) {
        final ThreadSafeBitSet ret = new ThreadSafeBitSet(reader.getNumDocs());
        final int[] docIdBuffer = new int[64]; // 64 instead of BUFFER_SIZE to be consistent with fillBitSet.
        try (
            final StringTermIterator iter = reader.getStringTermIterator(field);
            final DocIdStream dis = reader.getDocIdStream()
        ) {
            while (iter.next()) {
                dis.reset(iter);
                fillBitSetUsingBuffer(dis, ret, docIdBuffer);
            }
        }
        return ret;
    }

    public static FieldsCardinalityMetadata.FieldInfo calculateFieldCardinality(
            final TermIterator iter, final DocIdStream dis, final int size) {
        final int[] docIdBuffer = new int[BUFFER_SIZE];
        final FastBitSet single = new FastBitSet(size);
        final FastBitSet multi = new FastBitSet(size);
        int singles = 0;
        int multiples = 0;
        while (iter.next()) {
            dis.reset(iter);
            while(true) {
                final int n = dis.fillDocIdBuffer(docIdBuffer);
                for (int i = 0; i < n; i++) {
                    final int doc = docIdBuffer[i];
                    if (!single.get(doc)) {
                        single.set(doc);
                        singles++;
                    } else if (!multi.get(doc)) {
                        multi.set(doc);
                        multiples++;
                    }
                }
                if (n < docIdBuffer.length) {
                    break;
                }
            }
        }
        final boolean zero = size > singles;
        final boolean one = singles > multiples;
        final boolean two = multiples > 0;
        return new FieldsCardinalityMetadata.FieldInfo(zero, one, two);
    }

    public static FieldsCardinalityMetadata.FieldInfo cacheIntFieldCardinality(final String field, final FlamdexReader reader) {
        try (
                final TermIterator iter = reader.getIntTermIterator(field);
                final DocIdStream dis = reader.getDocIdStream()
        ) {
            return calculateFieldCardinality(iter, dis, reader.getNumDocs());
        }
    }

    public static FieldsCardinalityMetadata.FieldInfo cacheStringFieldCardinality(final String field, final FlamdexReader reader) {
        try (
                final TermIterator iter = reader.getStringTermIterator(field);
                final DocIdStream dis = reader.getDocIdStream()
        ) {
            return calculateFieldCardinality(iter, dis, reader.getNumDocs());
        }
    }

    private static void fillBitSetUsingBuffer(final DocIdStream dis, final ThreadSafeBitSet ret, final int[] docIdBuffer) {
        while (true) {
            final int n = dis.fillDocIdBuffer(docIdBuffer);
            for (int i = 0; i < n; ++i) {
                ret.set(docIdBuffer[i]);
            }
            if (n < docIdBuffer.length) {
                break;
            }
        }
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

    private static void cacheIntFieldRegex(
            final String field,
            final FlamdexReader reader,
            final Automaton automaton,
            final ThreadSafeBitSet ret) {
        try (final IntTermIterator iter = reader.getUnsortedIntTermIterator(field);
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
    private static void cacheStringFieldRegex(
            final String field,
            final FlamdexReader reader,
            final Automaton automaton,
            final ThreadSafeBitSet ret) {
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

    // return empty string if not found
    private static FieldType getFieldType(final FlamdexReader reader, final String field) {
        final boolean isIntField = reader.getIntFields().contains(field);
        final boolean isStrField = reader.getStringFields().contains(field);
        if (isIntField) {
            return FieldType.INT;
        } else if (isStrField) {
            return FieldType.STR;
        } else {
            return FieldType.NULL;
        }
    }

    public static ThreadSafeBitSet cacheFieldEqual(final String field1, final String field2, final FlamdexReader reader) {
        final ThreadSafeBitSet ret = new ThreadSafeBitSet(reader.getNumDocs());
        if (reader.getIntFields().contains(field1) && reader.getIntFields().contains(field2)) {
            cacheIntFieldEqual(field1, field2, reader, ret);
        } else if (reader.getStringFields().contains(field1) && reader.getStringFields().contains(field2)) {
            cacheStringFieldEqual(field1, field2, reader, ret);
        } else {
            final FieldType type1 = getFieldType(reader, field1);
            final FieldType type2 = getFieldType(reader, field2);
            if (type1 != FieldType.NULL && type2 != FieldType.NULL && type1 != type2 ) { // incompatible field
                throw new IllegalArgumentException(String.format("incompatible fields found in fieldequal: [%s -> %s], [%s -> %s]", field1, type1, field2, type2));
            }
        }
        return ret;
    }

    private static void cacheIntFieldEqual(
            final String field1,
            final String field2,
            final FlamdexReader reader,
            final ThreadSafeBitSet ret) {
        try (final IntTermIterator iter1 = reader.getIntTermIterator(field1);
             final IntTermIterator iter2 = reader.getIntTermIterator(field2);
             final DocIdStreamIterator docIdIter1 = new DocIdStreamIterator(reader.getDocIdStream(), BUFFER_SIZE);
             final DocIdStreamIterator docIdIter2 = new DocIdStreamIterator(reader.getDocIdStream(), BUFFER_SIZE)) {
            if (iter1.next() && iter2.next()) {
                while (true) {
                    final long term1 = iter1.term();
                    final long term2 = iter2.term();
                    final long compareResult = term1-term2;
                    if (compareResult == 0) {
                        docIdIter1.reset(iter1);
                        docIdIter2.reset(iter2);
                        mergeAndFillBitSet(docIdIter1, docIdIter2, ret);
                        if (!iter1.next()) {
                            break;
                        }
                        if (!iter2.next()) {
                            break;
                        }
                    } else if (compareResult < 0) {
                        if (!iter1.next()) {
                            break;
                        }
                    } else {
                        if (!iter2.next()) {
                            break;
                        }
                    }
                }
            }
        }
    }

    private static void cacheStringFieldEqual(
            final String field1,
            final String field2,
            final FlamdexReader reader,
            final ThreadSafeBitSet ret) {
        try (final StringTermIterator iter1 = reader.getStringTermIterator(field1);
             final StringTermIterator iter2 = reader.getStringTermIterator(field2);
             final DocIdStreamIterator docIdIter1 = new DocIdStreamIterator(reader.getDocIdStream(), BUFFER_SIZE);
             final DocIdStreamIterator docIdIter2 = new DocIdStreamIterator(reader.getDocIdStream(), BUFFER_SIZE)) {
            if (iter1.next() && iter2.next()) {
                while (true) {
                    final String term1 = iter1.term();
                    final String term2 = iter2.term();
                    final int compareResult = term1.compareTo(term2);
                    if (compareResult == 0) {
                        docIdIter1.reset(iter1);
                        docIdIter2.reset(iter2);
                        mergeAndFillBitSet(docIdIter1, docIdIter2, ret);
                        if (!iter1.next()) {
                            break;
                        }
                        if (!iter2.next()) {
                            break;
                        }
                    } else if (compareResult < 0) {
                        if (!iter1.next()) {
                            break;
                        }
                    } else {
                        if (!iter2.next()) {
                            break;
                        }
                    }
                }
            }
        }
    }

    private static void mergeAndFillBitSet(final DocIdStreamIterator docIdStreamIter1, final DocIdStreamIterator docIdStreamIter2, final ThreadSafeBitSet bitSet) {
        while (docIdStreamIter1.hasElement() && docIdStreamIter2.hasElement()) {
            final int docId1 = docIdStreamIter1.docId();
            final int docId2 = docIdStreamIter2.docId();
            if (docId1 == docId2) {
                bitSet.set(docId1);
                docIdStreamIter1.advance();
                docIdStreamIter2.advance();
            } else if (docId1 < docId2) {
                docIdStreamIter1.advance();
            } else {
                docIdStreamIter2.advance();
            }
        }
    }

    private static void setLongMetricsForDocs(final long[] array, final DocIdStream dis, final long metric) {
        final int[] docIdBuf = new int[BUFFER_SIZE];

        while (true) {
            final int n = dis.fillDocIdBuffer(docIdBuf);
            for (int i = 0; i < n; ++i) {
                array[docIdBuf[i]] = metric;
            }

            if (n < BUFFER_SIZE) {
                break;
            }
        }
    }

    public static LongArrayIntValueLookup cacheRegExpCapturedLong(final String field, final FlamdexReader reader, final Pattern regexPattern, final int matchIndex) {
        final long[] array = new long[reader.getNumDocs()];
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        try (final StringTermIterator iter = reader.getStringTermIterator(field);
             final DocIdStream dis = reader.getDocIdStream()) {
            while (iter.next()) {
                final Matcher matcher = regexPattern.matcher(iter.term());
                if (matcher.matches()) {
                    long metric;
                    try {
                        metric = Long.parseLong(matcher.group(matchIndex));
                    } catch (final NumberFormatException e) {
                        metric = 0L;
                    }
                    dis.reset(iter);
                    setLongMetricsForDocs(array, dis, metric);
                    min = Math.min(min, metric);
                    max = Math.max(max, metric);
                }
            }
        }

        return new LongArrayIntValueLookup(array, min, max);
    }

    public static long getIntTotalDocFreq(final FlamdexReader r, final String field) {
        long totalDocFreq = 0L;
        try (IntTermIterator iter = r.getUnsortedIntTermIterator(field)) {
            while (iter.next()) {
                totalDocFreq += iter.docFreq();
            }
        }
        return totalDocFreq;
    }

    public static long getStringTotalDocFreq(final FlamdexReader r, final String field) {
        long totalDocFreq = 0L;
        try (StringTermIterator iter = r.getStringTermIterator(field)) {
            while (iter.next()) {
                totalDocFreq += iter.docFreq();
            }
        }
        return totalDocFreq;
    }

    private enum FieldType {
        INT, STR, NULL
    }
}
