package com.indeed.imhotep.service;

import com.google.common.base.Charsets;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.util.io.VIntUtils;
import org.apache.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

/**
 * @author jplaisance
 */
public final class TGSBuffer {
    private static final Logger log = Logger.getLogger(TGSBuffer.class);

    final long[] stats;
    private final FTGSIterator iterator;
    private final boolean iteratorIsRaw;
    private boolean fieldIsIntType;

    private byte[] previousTermBytes = new byte[100];
    private int previousTermLength;
    private byte[] currentTermBytes = new byte[100];
    private long previousTermInt;

    public TGSBuffer(FTGSIterator iterator, int numStats) {
        this.iterator = iterator;
        iteratorIsRaw = iterator instanceof RawFTGSIterator;
        stats = new long[numStats];
    }

    public void resetField(boolean nextFieldIsIntType) {
        fieldIsIntType = nextFieldIsIntType;
        previousTermInt = -1;
        previousTermLength = 0;
    }

    public void resetBufferToCurrentTGS(DataOutput out) throws IOException {
        resetBufferToCurrentTGS(out, true);
    }

    public void resetBufferToCurrentTGS(DataOutput out, boolean writeGroups) throws IOException {
        int previousGroupId = -1;
        final int currentTermLength;
        final long currentTermInt;
        if (iteratorIsRaw) {
            final RawFTGSIterator rawBuffer = (RawFTGSIterator)iterator;
            if (fieldIsIntType) {
                currentTermInt = rawBuffer.termIntVal();
                currentTermLength = 0;
            } else {
                currentTermLength = rawBuffer.termStringLength();
                // termStringBytes() returns a reference so this copies the bytes instead of hanging on to it
                switchBytesTerm(rawBuffer.termStringBytes(), currentTermLength);
                currentTermInt = 0;
            }
        } else {
            if (fieldIsIntType) {
                currentTermInt = iterator.termIntVal();
                currentTermLength = 0;
            } else {
                final byte[] bytes = iterator.termStringVal().getBytes(Charsets.UTF_8);
                switchBytesTerm(bytes, bytes.length);
                currentTermLength = bytes.length;
                currentTermInt = 0;
            }
        }
        if (!writeGroups || iterator.nextGroup()) {
            if (fieldIsIntType) {
                if (previousTermInt == -1 && currentTermInt == previousTermInt) {
                    //still decodes to 0 but allows reader to distinguish between end of field and delta of zero
                    out.write(0x80);
                    out.write(0);
                } else {
                    writeVLong(out, currentTermInt - previousTermInt);
                }
                previousTermInt = currentTermInt;
            } else {
                final int pLen = prefixLen(previousTermBytes, currentTermBytes, Math.min(previousTermLength, currentTermLength));
                writeVLong(out, (previousTermLength - pLen) + 1);
                writeVLong(out, currentTermLength - pLen);
                out.write(currentTermBytes, pLen, currentTermLength - pLen);
                previousTermBytes = copyInto(currentTermBytes, currentTermLength, previousTermBytes);
                previousTermLength = currentTermLength;
            }
            writeSVLong(out, iterator.termDocFreq());
            if (writeGroups) {
                do {
                    final int groupId = iterator.group();
                    writeVLong(out, groupId - previousGroupId);
                    previousGroupId = groupId;
                    iterator.groupStats(stats);
                    for (long stat : stats) {
                        writeSVLong(out, stat);
                    }
                } while (iterator.nextGroup());
            }
            out.write(0);
        }
    }

    public void resetBufferToCurrentTermOnly(DataOutput out) throws IOException {
        resetBufferToCurrentTGS(out, false);
    }

    private void switchBytesTerm(byte[] termBytes, int termLength) {
        currentTermBytes = copyInto(termBytes, termLength, currentTermBytes);
    }

    private static void writeVLong(DataOutput out, long i) throws IOException {
        VIntUtils.writeVInt64(out, i);
    }

    private static void writeSVLong(DataOutput out, long i) throws IOException {
        VIntUtils.writeSVInt64(out, i);
    }

    private static byte[] copyInto(final byte[] src, final int srcLen, byte[] dest) {
        dest = ensureCap(dest, srcLen);
        System.arraycopy(src, 0, dest, 0, srcLen);
        return dest;
    }

    private static byte[] ensureCap(final byte[] b, final int len) {
        if (b == null) return new byte[Math.max(len, 16)];
        if (b.length >= len) return b;
        return new byte[Math.max(b.length*2, len)];
    }

    private static int prefixLen(final byte[] a, final byte[] b, final int max) {
        for (int i = 0; i < max; i++) {
            if (a[i] != b[i]) return i;
        }
        return max;
    }
}
