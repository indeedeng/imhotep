package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

public final class DocIdMetric implements IntValueLookup {
    private final int numDocs;

    public DocIdMetric(final int numDocs) {
        this.numDocs = numDocs;
    }

    @Override
    public long getMin() {
        return 0;
    }

    @Override
    public long getMax() {
        return numDocs-1;
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
        for (int i = 0; i < n; i++) {
            values[i] = docIds[i];
        }
    }

    @Override
    public long memoryUsed() {
        return 0;
    }

    @Override
    public void close() {
    }
}
