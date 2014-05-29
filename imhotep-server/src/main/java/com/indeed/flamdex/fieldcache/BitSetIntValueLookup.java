package com.indeed.flamdex.fieldcache;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;

/**
 * @author jsgroth
 */
public final class BitSetIntValueLookup implements IntValueLookup {
    private FastBitSet lookupBitSet;
    private final long min;
    private final long max;

    public BitSetIntValueLookup(FastBitSet lookupBitSet) {
        this(lookupBitSet, 0, 1);
    }

    public BitSetIntValueLookup(FastBitSet lookupBitSet, long min, long max) {
        this.lookupBitSet = lookupBitSet;
        this.min = min;
        this.max = max;
    }

    @Override
    public long getMin() {
        return min;
    }

    @Override
    public long getMax() {
        return max;
    }

    @Override
    public final void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = lookupBitSet.get(docIds[i]) ? 1 : 0;
        }
    }

    @Override
    public long memoryUsed() {
        return lookupBitSet.memoryUsage();
    }

    @Override
    public void close() {
        lookupBitSet = null;
    }
}
