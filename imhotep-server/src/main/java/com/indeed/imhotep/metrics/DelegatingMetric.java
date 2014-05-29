package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * A "pointer" to another metric farther down the stack.
 * @author dwahler
 */
public class DelegatingMetric implements IntValueLookup {
    private final IntValueLookup inner;

    public DelegatingMetric(IntValueLookup inner) {
        this.inner = inner;
    }

    @Override
    public long getMin() {
        return inner.getMin();
    }

    @Override
    public long getMax() {
        return inner.getMax();
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        inner.lookup(docIds, values, n);
    }

    @Override
    public long memoryUsed() {
        return 0;
    }

    @Override
    public void close() {
        // nothing
    }
}
