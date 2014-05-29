package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * User: arun
 * Note: Since the result is stored as an int, Log(0) will be INTEGER.MIN_VALUE.(JLS §5.1.3)
 */
public final class Log implements IntValueLookup {

    private final IntValueLookup operand;
    private final int scaleFactor;
    private final double logScaleFactor;

    public Log(IntValueLookup operand, int scaleFactor) {
        this.operand = operand;
        this.scaleFactor = scaleFactor;
        this.logScaleFactor = Math.log(scaleFactor);
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
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            values[i] = (long) ((Math.log(values[i]) - logScaleFactor) * scaleFactor);
        }
    }

    @Override
    public long memoryUsed() {
        return operand.memoryUsed();
    }

    @Override
    public void close() {
        operand.close();
    }
}
