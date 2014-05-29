package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

import java.util.Arrays;

/**
 * @author jsgroth
 */
public class Constant implements IntValueLookup {
    private final long val;

    public Constant(long val) {
        this.val = val;
    }

    @Override
    public long getMin() {
        return val;
    }

    @Override
    public long getMax() {
        return val;
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        Arrays.fill(values, 0, n, val);
    }

    @Override
    public long memoryUsed() {
        return 0L;
    }

    @Override
    public void close() {
    }
}
