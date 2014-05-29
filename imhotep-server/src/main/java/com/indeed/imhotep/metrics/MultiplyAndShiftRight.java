package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class MultiplyAndShiftRight extends AbstractBinaryOperator {
    private static final Logger log = Logger.getLogger(MultiplyAndShiftRight.class);
    private final int shift;

    public MultiplyAndShiftRight(final IntValueLookup a, final IntValueLookup b, int shift) {
        super(a, b);
        this.shift = shift;
    }

    protected void combine(final long[] values, final long[] buffer, final int n) {
        for (int i = 0; i < n; i++) {
            final long result = values[i] * buffer[i];
            values[i] = result >> shift;
        }
    }
}
