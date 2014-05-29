package com.indeed.flamdex.fieldcache;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * @author jsgroth
 */
public final class ByteArrayIntValueLookup implements IntValueLookup {
    private byte[] lookupArray;
    private final long min;
    private final long max;

    public ByteArrayIntValueLookup(byte[] lookupArray) {
        this.lookupArray = lookupArray;
        long tmin = Long.MAX_VALUE;
        long tmax = Long.MIN_VALUE;
        for (final byte bval : lookupArray) {
            final long val = bval & 0xFFL;
            tmin = Math.min(tmin, val);
            tmax = Math.max(tmax, val);
        }
        min = tmin;
        max = tmax;
    }

    public ByteArrayIntValueLookup(byte[] lookupArray, long min, long max) {
        this.lookupArray = lookupArray;
        this.min = min;
        this.max = max;
    }

    @Override
    public long getMin() {
        return min;
    }

    @Override
    public long getMax() {
        return max;
    }

    @Override
    public final void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = lookupArray[docIds[i]] & 0xFF;
        }
    }

    @Override
    public long memoryUsed() {
        return lookupArray.length;
    }

    @Override
    public void close() {
        lookupArray = null;
    }
}
