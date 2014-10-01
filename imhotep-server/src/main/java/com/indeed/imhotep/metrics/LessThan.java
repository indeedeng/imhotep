package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class LessThan extends AbstractBinaryOperator {
    private static final Logger log = Logger.getLogger(Equal.class);

    public LessThan(IntValueLookup a, IntValueLookup b) {
        super(a, b);
    }

    @Override
    protected void combine(long[] values, long[] buffer, int n) {
        for (int i = 0; i < n; i++) {
            values[i] = (values[i] < buffer[i]) ? 1 : 0;
        }
    }
}
