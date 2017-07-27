package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class GreaterThan extends AbstractBinaryOperator {
    private static final Logger log = Logger.getLogger(GreaterThan.class);

    public GreaterThan(final IntValueLookup a, final IntValueLookup b) {
        super(a, b);
    }

    @Override
    public long getMin() {
        return 0;
    }

    @Override
    public long getMax() {
        return 1;
    }

    @Override
    protected void combine(final long[] values, final long[] buffer, final int n) {
        for (int i = 0; i < n; i++) {
            values[i] = (values[i] > buffer[i]) ? 1 : 0;
        }
    }
}
