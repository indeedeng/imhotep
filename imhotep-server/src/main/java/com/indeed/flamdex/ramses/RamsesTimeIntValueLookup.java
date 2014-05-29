package com.indeed.flamdex.ramses;

import com.indeed.flamdex.api.IntValueLookup;

import java.util.Arrays;

/**
 * @author jsgroth
 */
public class RamsesTimeIntValueLookup implements IntValueLookup {
    private final long memoryOverhead;

    private int[] timeUpperBits;
    private int[] docIdBoundaries;
    private byte[] timeLowerBits;

    public RamsesTimeIntValueLookup(int[] timeUpperBits, int[] docIdBoundaries, byte[] timeLowerBits, long memoryOverhead) {
        this.timeUpperBits = timeUpperBits;
        this.docIdBoundaries = docIdBoundaries;
        this.timeLowerBits = timeLowerBits;
        this.memoryOverhead = memoryOverhead;
    }

    @Override
    public long getMin() {
        return (timeUpperBits[0] << 8) | (timeLowerBits[0] + 128);
    }

    @Override
    public long getMax() {
        return (timeUpperBits[timeUpperBits.length - 1] << 8) | (timeLowerBits[timeLowerBits.length - 1] + 128);
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = docIdToValue(timeUpperBits, docIdBoundaries, timeLowerBits, docIds[i]);
        }
    }

    @Override
    public long memoryUsed() {
        return memoryOverhead;
    }

    @Override
    public void close() {
        timeUpperBits = null;
        docIdBoundaries = null;
        timeLowerBits = null;
    }

    private static int gteBinarySearch(int[] a, int v) {
        final int ret = Arrays.binarySearch(a, v);
        if (ret >= 0) return ret;
        return -(ret + 2);
    }

    private static int docIdToValue(final int[] upperBits, final int[] startDocIds, final byte[] lowerBits, final int docId) {
        if (docId >= startDocIds[startDocIds.length - 1]) {
            return (upperBits[startDocIds.length - 1] << 8) | (128 + lowerBits[docId]);
        }
        return (upperBits[gteBinarySearch(startDocIds, docId)] << 8) | (128 + lowerBits[docId]);
    }
}
