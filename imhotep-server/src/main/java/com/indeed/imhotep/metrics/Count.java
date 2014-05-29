package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

import java.util.Arrays;

/**
 * @author jsgroth
 */
public class Count implements IntValueLookup {
    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        Arrays.fill(values, 0, n, 1);
    }

    @Override
    public long getMin() {
        return 1;
    }

    @Override
    public long getMax() {
        return 1;
    }

    @Override
    public long memoryUsed() {
        return 0L;
    }

    @Override
    public void close() {
    }
}
