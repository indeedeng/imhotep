package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class NotEqual extends AbstractBinaryOperator {
    private static final Logger log = Logger.getLogger(Equal.class);

    public NotEqual(IntValueLookup a, IntValueLookup b) {
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
    protected void combine(long[] values, long[] buffer, int n) {
        for (int i = 0; i < n; i++) {
            values[i] = (values[i] != buffer[i]) ? 1 : 0;
        }
    }
}
