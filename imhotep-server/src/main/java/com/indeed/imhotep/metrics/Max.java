package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * @author jwolfe
 */
public class Max extends AbstractBinaryOperator {
    public Max(IntValueLookup a, IntValueLookup b) {
        super(a, b);
    }

    @Override
    protected void combine(long[] values, long[] buffer, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = Math.max(values[i], buffer[i]);
        }
    }
}
