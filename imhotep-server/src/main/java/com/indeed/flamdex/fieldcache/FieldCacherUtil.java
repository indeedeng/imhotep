package com.indeed.flamdex.fieldcache;

import com.google.common.base.Charsets;
import com.google.common.io.Closer;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.indeed.flamdex.AbstractFlamdexReader.MinMax;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.util.core.Pair;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.mmap.BufferResource;
import com.indeed.util.mmap.IntArray;
import com.indeed.util.mmap.NativeBuffer;
import com.indeed.util.mmap.ZeroCopyOutputStream;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Created by darren on 3/9/15.
 */
public class FieldCacherUtil {
    private FieldCacherUtil() {
    }

    private static final Logger log = Logger.getLogger(FieldCacherUtil.class);

    public static StringValueLookup newStringValueLookup(final String field, final FlamdexReader r) throws IOException {
        final Pair<? extends BufferResource, ? extends BufferResource> pair = buildStringValueLookup(field, r);
        return new MMapStringValueLookup(pair.getFirst(), pair.getSecond());
    }

    private static Pair<? extends BufferResource, ? extends BufferResource> buildStringValueLookup(final String field,
                                                                                                   final FlamdexReader r) throws IOException {
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
                    if (n < docIdBuffer.length) {
                        break;
                    }
                }
            }
            valuesOut.flush();
            final NativeBuffer buffer = valuesFileOut.getBuffer().realloc(valuesFileOut.position());
            return Pair.of(offsets, buffer);
        } catch (final Throwable t) {
            closer.close();
            throw Throwables2.propagate(t, IOException.class);
        } finally {
            Closeables2.closeQuietly(stringTermDocIterator, log);
        }
    }

    public static FieldCacher getCacherForField(final String field, final FlamdexReader r, final MinMax minMax) {
        final long[] minMaxTerm = FlamdexUtils.getMinMaxTerm(field, r);
        final long minTermVal = minMaxTerm[0];
        final long maxTermVal = minMaxTerm[1];
        minMax.min = minTermVal;
        minMax.max = maxTermVal;

        if (minTermVal >= 0 && maxTermVal <= 1) {
            return FieldCacher.BITSET;
        } else if (minTermVal >= 0 && maxTermVal <= 255) {
            return FieldCacher.BYTE;
        } else if (minTermVal >= Byte.MIN_VALUE && maxTermVal <= Byte.MAX_VALUE) {
            return FieldCacher.SIGNED_BYTE;
        } else if (minTermVal >= 0 && maxTermVal <= 65535) {
            return FieldCacher.CHAR;
        } else if (minTermVal >= Short.MIN_VALUE && maxTermVal <= Short.MAX_VALUE) {
            return FieldCacher.SHORT;
        } else if (minTermVal >= Integer.MIN_VALUE && maxTermVal <= Integer.MAX_VALUE) {
            return FieldCacher.INT;
        } else {
            return FieldCacher.LONG;
        }
    }

    public static NativeFlamdexFieldCacher getNativeCacherForField(final String field,
                                                                   final SimpleFlamdexReader r,
                                                                   final MinMax minMax) {
        final long[] minMaxTerm = FlamdexUtils.getMinMaxTerm(field, r);
        final long minTermVal = minMaxTerm[0];
        final long maxTermVal = minMaxTerm[1];
        minMax.min = minTermVal;
        minMax.max = maxTermVal;

        if (minTermVal >= 0 && maxTermVal <= 1) {
            return NativeFlamdexFieldCacher.BITSET;
        } else if (minTermVal >= 0 && maxTermVal <= 255) {
            return NativeFlamdexFieldCacher.BYTE;
        } else if (minTermVal >= Byte.MIN_VALUE && maxTermVal <= Byte.MAX_VALUE) {
            return NativeFlamdexFieldCacher.SIGNED_BYTE;
        } else if (minTermVal >= 0 && maxTermVal <= 65535) {
            return NativeFlamdexFieldCacher.CHAR;
        } else if (minTermVal >= Short.MIN_VALUE && maxTermVal <= Short.MAX_VALUE) {
            return NativeFlamdexFieldCacher.SHORT;
        } else if (minTermVal >= Integer.MIN_VALUE && maxTermVal <= Integer.MAX_VALUE) {
            return NativeFlamdexFieldCacher.INT;
        } else {
            return NativeFlamdexFieldCacher.LONG;
        }
    }
}
