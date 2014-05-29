package com.indeed.imhotep.metrics;

import com.google.common.primitives.Longs;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

/**
 * A precomputed, cached version of an arbitrary metric
 * @author dwahler
 */
public class CachedMetric implements IntValueLookup {
    private final MemoryReserver memory;
    private long[] values;
    private long min, max;

    public CachedMetric(IntValueLookup original, int numDocs, MemoryReserver memory) throws ImhotepOutOfMemoryException {
        this.memory = memory;

        if (!memory.claimMemory(numDocs * 8L)) {
            throw new ImhotepOutOfMemoryException();
        }

        this.values = new long[numDocs];
        fillValues(original);
    }

    private void fillValues(IntValueLookup original) {
        final int BUFFER_SIZE = 8192;
        final int[] idBuffer = new int[BUFFER_SIZE];
        final long[] valBuffer = new long[BUFFER_SIZE];

        for (int start = 0; start < values.length; start += BUFFER_SIZE) {
            final int end = Math.min(values.length, start+BUFFER_SIZE), n = end-start;
            for (int i = 0; i < n; i++) {
                idBuffer[i] = start + i;
            }
            original.lookup(idBuffer, valBuffer, n);
            System.arraycopy(valBuffer, 0, values, start, n);
        }

        min = Longs.min(values);
        max = Longs.max(values);
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
    public void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; i++) {
            values[i] = this.values[docIds[i]];
        }
    }

    @Override
    public long memoryUsed() {
        return 8L * values.length;
    }

    @Override
    public void close() {
        final long bytesToFree = memoryUsed();
        values = null;
        memory.releaseMemory(bytesToFree);
    }
}
