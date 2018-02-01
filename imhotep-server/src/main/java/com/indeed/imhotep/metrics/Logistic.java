package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * f(x) = scaleUp / (1 + e^(-x / scaleDown))
 * f(x) is monotonic but if it's increasing or decreasing depends on scaleUp and scaleDown
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
        // since f(x) is monotonic min is on range border.
        return Math.min(eval(operand.getMin()), eval(operand.getMax()));
    }

    @Override
    public long getMax() {
        // since f(x) is monotonic max is on range border.
        return Math.max(eval(operand.getMin()), eval(operand.getMax()));
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            values[i] = eval(values[i]);
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

    private long eval(final long value) {
        final double x = value / scaleDown;
        return (long)(scaleUp/(1+Math.exp(-x)));
    }
}
