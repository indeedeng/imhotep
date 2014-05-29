package com.indeed.flamdex.fieldcache;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * @author jsgroth
 */
public final class ShortArrayIntValueLookup implements IntValueLookup {
    private short[] lookupArray;
    private final long min;
    private final long max;

    public ShortArrayIntValueLookup(short[] lookupArray) {
        this.lookupArray = lookupArray;
        long tmin = Long.MAX_VALUE;
        long tmax = Long.MIN_VALUE;
        for (final long val : lookupArray) {
            tmin = Math.min(tmin, val);
            tmax = Math.max(tmax, val);
        }
        min = tmin;
        max = tmax;
    }

    public ShortArrayIntValueLookup(short[] lookupArray, long min, long max) {
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
            values[i] = lookupArray[docIds[i]];
        }
    }

    @Override
    public long memoryUsed() {
        return 2L * lookupArray.length;
    }

    @Override
    public void close() {
        lookupArray = null;
    }
}
