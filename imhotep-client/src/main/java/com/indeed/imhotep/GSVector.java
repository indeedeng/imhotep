package com.indeed.imhotep;

import com.indeed.imhotep.api.RawFTGSIterator;
import org.apache.log4j.Logger;

import java.util.Arrays;

/**
 * @author jplaisance
 */
public final class GSVector {
    private static final Logger log = Logger.getLogger(GSVector.class);

    long bitset1;
    final long[] bitset2 = new long[64];
    final long[] metrics;

    private final int numStats;
    private final long[] statBuf;

    private int baseGroup = -1;
    int numGroups;

    public GSVector(final int numStats) {
        this.numStats = numStats;
        metrics = new long[numStats*4096];
        statBuf = new long[numStats];
    }

    //precondition: nextGroup has been called and returned true
    public boolean readFromFtgs(RawFTGSIterator ftgs) {
        numGroups = 0;
        while (bitset1 != 0) {
            final long lsb1 = bitset1 & -bitset1;
            bitset1 ^= lsb1;
            final int index1 = Long.bitCount(lsb1-1);
            bitset2[index1] = 0;
        }
        int group = ftgs.group();
        baseGroup = group&0xFFFFF000;
        group -= baseGroup;
        do {
            ftgs.groupStats(statBuf);
            System.arraycopy(statBuf, 0, metrics, (baseGroup+group)*numStats, numStats);
            final int bitset2index = group>>>6;
            bitset1 |= 1L<<bitset2index;
            bitset2[bitset2index] |= 1L<<(group&0x3F);
            numGroups++;
            if (!ftgs.nextGroup()) return false;
            group = ftgs.group()-baseGroup;
        } while (group < 4096);
        return true;
    }

    public void copy(GSVector other) {
        long otherBitset1 = other.bitset1;
        while (otherBitset1 != 0) {
            final long lsb1 = otherBitset1 & -otherBitset1;
            otherBitset1 ^= lsb1;
            final int index1 = Long.bitCount(lsb1-1);
            long otherBitset2 = other.bitset2[index1];
            bitset2[index1] = otherBitset2;
            while (otherBitset2 != 0) {
                final long lsb2 = otherBitset2 & -otherBitset2;
                otherBitset2 ^= lsb2;
                final int index2 = (index1<<6)+Long.bitCount(lsb2-1);
                System.arraycopy(other.metrics, index2*numStats, metrics, index2*numStats, numStats);
            }
        }
        numGroups = other.numGroups;
        baseGroup = other.baseGroup;
        bitset1 = other.bitset1;
    }

    public void reset() {
        while (bitset1 != 0) {
            final long lsb1 = bitset1 & -bitset1;
            bitset1 ^= lsb1;
            final int index1 = Long.bitCount(lsb1-1);
            while (bitset2[index1] != 0) {
                final long lsb2 = bitset2[index1] & -bitset2[index1];
                bitset2[index1] ^= lsb2;
                final int index2 = (index1<<6)+Long.bitCount(lsb2-1);
                Arrays.fill(metrics, index2*numStats, index2*numStats+numStats, 0);
            }
        }
        baseGroup = -1;
        numGroups = 0;
    }

    public void merge(GSVector other) {
        if (baseGroup == -1) baseGroup = other.baseGroup;
        if (baseGroup != other.baseGroup) throw new IllegalArgumentException();
        long otherBitset1 = other.bitset1;
        while (otherBitset1 != 0) {
            final long lsb1 = otherBitset1 & -otherBitset1;
            otherBitset1 ^= lsb1;
            final int index1 = Long.bitCount(lsb1-1);
            bitset2[index1] |= other.bitset2[index1];
            long otherBitset2 = other.bitset2[index1];
            while (otherBitset2 != 0) {
                final long lsb2 = otherBitset2 & -otherBitset2;
                otherBitset2 ^= lsb2;
                final int index2 = (index1<<6)+Long.bitCount(lsb2-1);
                for (int i = index2*numStats; i < index2*numStats+numStats; i++) {
                    metrics[i] += other.metrics[i];
                }
            }
        }
        bitset1 |= other.bitset1;
    }
}
