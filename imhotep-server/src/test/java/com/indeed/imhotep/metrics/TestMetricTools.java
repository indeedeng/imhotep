package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.fieldcache.LongArrayIntValueLookup;

public final class TestMetricTools {
    private TestMetricTools() {
    }

    public static IntValueLookup range(final long min, final long max) {
        if (min > max) {
            throw new IllegalArgumentException("min must be not greater than max");
        }
        return new LongArrayIntValueLookup(new long[0], min, max);
    }

    public static boolean isExactRange(final IntValueLookup lookup, final long min, final long max) {
        return (min == lookup.getMin())
                && (lookup.getMin() <= lookup.getMax())
                && (lookup.getMax() == max);
    }

    public static boolean isInRange(final IntValueLookup lookup, final long min, final long max) {
        return (min <= lookup.getMin())
                && (lookup.getMin() <= lookup.getMax())
                && (lookup.getMax() <= max);
    }
}
