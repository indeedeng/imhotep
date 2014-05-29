package com.indeed.flamdex.fieldcache;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * @author jsgroth
 */
public final class CharArrayIntValueLookup implements IntValueLookup {
    private char[] lookupArray;
    private final long min;
    private final long max;

    public CharArrayIntValueLookup(char[] lookupArray) {
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

    public CharArrayIntValueLookup(char[] lookupArray, long min, long max) {
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
