package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * @author jsgroth
 */
public abstract class AbstractBinaryOperator implements IntValueLookup {
    public static final int INITIAL_BUFFER_SIZE = 32;

    protected final IntValueLookup a;
    protected final IntValueLookup b;

    protected long[] buffer = new long[INITIAL_BUFFER_SIZE];

    protected AbstractBinaryOperator(IntValueLookup a, IntValueLookup b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public long getMin() {
        return Long.MIN_VALUE;
    }

    @Override
    public long getMax() {
        return Long.MAX_VALUE;
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        if (buffer.length < n) buffer = new long[n];
        a.lookup(docIds, values, n);
        b.lookup(docIds, buffer, n);
        combine(values, buffer, n);
    }

    protected abstract void combine(long[] values, long[] buffer, int n);

    @Override
    public long memoryUsed() {
        return a.memoryUsed() + b.memoryUsed();
    }

    @Override
    public void close() {
        a.close();
        b.close();
    }
}
