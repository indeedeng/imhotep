package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * 1/(1+e^-x)
 * @author jplaisance
 */
public final class Logistic implements IntValueLookup {
    private final IntValueLookup operand;
    private final double scaleDown;
    private final double scaleUp;

    public Logistic(final IntValueLookup operand, final double scaleDown, final double scaleUp) {
        this.operand = operand;
        this.scaleDown = scaleDown;
        this.scaleUp = scaleUp;
    }

    @Override
    public long getMin() {
        return 0;
    }

    @Override
    public long getMax() {
        return (long)scaleUp;
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            final double x = values[i] / scaleDown;
            values[i] = (long)(scaleUp/(1+Math.exp(-x)));
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
