package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * Fixed-point exponential function
 * @author dwahler
 */
public class Exponential implements IntValueLookup {
    private final IntValueLookup operand;
    private final int scaleFactor;

    public Exponential(IntValueLookup operand, int scaleFactor) {
        this.operand = operand;
        this.scaleFactor = scaleFactor;
    }

    @Override
    public long getMin() {
        return 0;
    }

    @Override
    public long getMax() {
        return scaleFactor;
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            double x = values[i] / (double) scaleFactor;
            double result = Math.exp(x);

            // the output is clamped to [Integer.MIN_VALUE, Integer.MAX_VALUE] (JLS §5.1.3)
            values[i] = (long) (result * scaleFactor);
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
