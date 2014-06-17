package com.indeed.imhotep;

import com.google.common.base.Charsets;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.RawFTGSIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author jsgroth
 */
public final class RawFTGSMerger extends AbstractFTGSMerger implements RawFTGSIterator {
    private final CharsetDecoder decoder = Charsets.UTF_8.newDecoder();

    private final RawFTGSIterator[] rawIteratorRefs;

    private byte[] currentTermBytes;
    private ByteBuffer byteBuffer;
    private int currentTermLength;
    private String termStringVal;

    public RawFTGSMerger(Collection<? extends RawFTGSIterator> iterators, int numStats, @Nullable Closeable doneCallback) {
        super(iterators, numStats, doneCallback);
        rawIteratorRefs = iterators.toArray(new RawFTGSIterator[iterators.size()]);
        currentTermBytes = new byte[100];
        byteBuffer = ByteBuffer.wrap(currentTermBytes);
        currentTermLength = 0;
        termStringVal = null;
    }

    @Override
    public String termStringVal() {
        if (termStringVal == null) {
            try {
                termStringVal = decoder.decode((ByteBuffer)byteBuffer.position(0).limit(currentTermLength)).toString();
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
        return termStringVal;
    }

    @Override
    public byte[] termStringBytes() {
        return currentTermBytes;
    }

    @Override
    public int termStringLength() {
        return currentTermLength;
    }

    @Override
    public boolean nextTerm() {
        for (int i = 0; i < numTermIterators; ++i) {
            final FTGSIterator itr = iterators[termIterators[i]];
            if (!itr.nextTerm()) {
                final int fi = termIteratorIndexes[i];
                swap(fieldIterators, fi, --numFieldIterators);
                for (int j = 0; j < numTermIterators; ++j) {
                    if (termIteratorIndexes[j] == numFieldIterators) {
                        termIteratorIndexes[j] = fi;
                    }
                }
            }
        }

        numTermIterators = 0;
        if (numFieldIterators == 0) return false;

        int newNumTermIterators = 0;
        if (fieldIsIntType) {
            long min = Long.MAX_VALUE;
            for (int i = 0; i < numFieldIterators; ++i) {
                final FTGSIterator itr = iterators[fieldIterators[i]];
                final long term = itr.termIntVal();
                if (term < min) {
                    newNumTermIterators = 1;
                    termIteratorIndexes[0] = i;
                    min = term;
                } else if (term == min) {
                    termIteratorIndexes[newNumTermIterators++] = i;
                }
            }
            termIntVal = min;
        } else {
            byte[] minTermBytes = null;
            int minTermLength = -1;
            for (int i = 0; i < numFieldIterators; ++i) {
                final RawFTGSIterator itr = rawIteratorRefs[fieldIterators[i]];
                final byte[] termBytes = itr.termStringBytes();
                final int termLength = itr.termStringLength();
                final int c;
                if (minTermBytes == null || (c = compareBytes(termBytes, termLength, minTermBytes, minTermLength)) < 0) {
                    newNumTermIterators = 1;
                    termIteratorIndexes[0] = i;
                    minTermBytes = termBytes;
                    minTermLength = termLength;
                } else if (c == 0) {
                    termIteratorIndexes[newNumTermIterators++] = i;
                }
            }
            if (currentTermBytes.length < minTermLength) {
                currentTermBytes = Arrays.copyOf(minTermBytes, Math.max(minTermLength, 2 * currentTermBytes.length));
                byteBuffer = ByteBuffer.wrap(currentTermBytes);
            } else {
                System.arraycopy(minTermBytes, 0, currentTermBytes, 0, minTermLength);
            }
            currentTermLength = minTermLength;
            termStringVal = null;
        }

        for (int i = 0; i < newNumTermIterators; ++i) {
            final int fi = termIteratorIndexes[i];
            final int index = fieldIterators[fi];
            termIterators[numTermIterators] = index;
            termIteratorIndexes[numTermIterators++] = fi;
        }
        termIteratorsRemaining = numTermIterators;
        for (int i = 0; i < termIteratorsRemaining; ++i) {
            final FTGSIterator itr = iterators[termIterators[i]];
            if (!itr.nextGroup()) {
                swap(termIterators, i, --termIteratorsRemaining);
                swap(termIteratorIndexes, i, termIteratorsRemaining);
                --i;
            }
        }
        accumulatedVec.reset();
        return true;
    }

    // this is a comparison of UTF-8 bytes that is wrong in the same way String.compareTo(String) is wrong
    public static int compareBytes(final byte[] b1, final int l1, final byte[] b2, final int l2) {
        for (int i = 0, j = 0; i < l1 && j < l2; i++, j++) {
            final int v1 = b1[i] & 0xFF;
            final int v2 = b2[j] & 0xFF;
            if (v1 != v2) {
                if (((v1 & 0xF0) == 0xF0 || (v2 & 0xF0) == 0xF0) && ((v1 & 0xF0) != (v2 & 0xF0))) {
                    if ((v1 & 0xF0) == 0xF0) return UTF8ToCodePoint(v2, b2, j + 1, l2) > 0xDFFF ? -1 : 1;
                    return UTF8ToCodePoint(v1, b1, i + 1, l1) > 0xDFFF ? 1 : -1;
                }
                return v1 - v2;
            }
        }
        return l1 - l2;
    }

    static int UTF8ToCodePoint(final int firstByte, final byte[] b, int off, final int len) {
        if (firstByte < 128) return firstByte;

        int cp;
        final int cpl;
        if ((firstByte & 0x20) == 0) {
            cpl = 1;
            cp = (firstByte & 0x1F) << 6;
        } else if ((firstByte & 0x10) == 0) {
            cpl = 2;
            cp = (firstByte & 0x0F) << 12;
        } else if ((firstByte & 0x08) == 0) {
            cpl = 3;
            cp = (firstByte & 0x07) << 18;
        } else if ((firstByte & 0x04) == 0) {
            cpl = 4;
            cp = (firstByte & 0x03) << 24;
        } else if ((firstByte & 0x02) == 0) {
            cpl = 5;
            cp = (firstByte & 0x01) << 30;
        } else {
            throw new RuntimeException("invalid UTF-8");
        }

        if (off + cpl > len) throw new RuntimeException("invalid UTF-8");

        for (int k = cpl - 1; k >= 0; --k) {
            cp |= ((b[off++] & 0x3F) << (6*k));
        }

        return cp;
    }
}
