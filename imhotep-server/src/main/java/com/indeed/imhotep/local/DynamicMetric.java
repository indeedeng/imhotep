package com.indeed.imhotep.local;

import java.io.Serializable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.indeed.flamdex.api.IntValueLookup;

@VisibleForTesting
public class DynamicMetric implements IntValueLookup, Serializable {
    private static final long serialVersionUID = 1L;
    private final int[] values;

    public DynamicMetric(int size) {
        this.values = new int[size];
    }

    @Override
    public long getMin() {
        return Ints.min(values);
    }

    @Override
    public long getMax() {
        return Ints.max(values);
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; i++) {
            values[i] = this.values[docIds[i]];
        }
    }

    @Override
    public long memoryUsed() {
        return 4L * values.length;
    }

    @Override
    public void close() {
        // simply popping this from the metric stack doesn't have any effect
    }

    public void add(int doc, int delta) {
        // TODO optimize this to remove branches
        final long newValue = (long) values[doc] + (long) delta;
        if (newValue < Integer.MIN_VALUE) {
            values[doc] = Integer.MIN_VALUE;
        } else if (newValue > Integer.MAX_VALUE) {
            values[doc] = Integer.MAX_VALUE;
        } else {
            values[doc] = (int) newValue;
        }
    }
    
    public void set(int doc, int value) {
        values[doc] = value;
    }

    public int lookupSingleVal(int docId) {
        return this.values[docId];
    }

}