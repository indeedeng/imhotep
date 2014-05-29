package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * @author jwolfe
 */
public class Min extends AbstractBinaryOperator {
    public Min(IntValueLookup a, IntValueLookup b) {
        super(a, b);
    }

    @Override
    protected void combine(long[] values, long[] buffer, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = Math.min(values[i], buffer[i]);
        }
    }
}
