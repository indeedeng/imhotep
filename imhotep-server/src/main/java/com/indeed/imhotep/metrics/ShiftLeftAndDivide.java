package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class ShiftLeftAndDivide extends AbstractBinaryOperator {
    private static final Logger log = Logger.getLogger(ShiftLeftAndDivide.class);
    private final int shift;

    public ShiftLeftAndDivide(final IntValueLookup a, final IntValueLookup b, int shift) {
        super(a, b);
        this.shift = shift;
    }

    protected void combine(final long[] values, final long[] buffer, final int n) {
        for (int i = 0; i < n; i++) {
            if (buffer[i] == 0) {
                values[i] = 0;
            } else {
                final long result = values[i] << shift;
                values[i] = result / buffer[i];
            }
        }
    }
}
