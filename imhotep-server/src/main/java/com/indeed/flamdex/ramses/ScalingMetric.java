package com.indeed.flamdex.ramses;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * @author jsgroth
 */
public class ScalingMetric implements IntValueLookup {
    private final IntValueLookup metric;
    private final int scaleFactor;

    public ScalingMetric(IntValueLookup metric, int scaleFactor) {
        this.metric = metric;
        this.scaleFactor = scaleFactor;
    }

    @Override
    public long getMin() {
        return metric.getMin() * scaleFactor;
    }

    @Override
    public long getMax() {
        return metric.getMax() * scaleFactor;
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        metric.lookup(docIds, values, n);
        for (int i = 0; i < n; ++i) {
            values[i] *= scaleFactor;
        }
    }

    @Override
    public long memoryUsed() {
        return metric.memoryUsed();
    }

    @Override
    public void close() {
        metric.close();
    }
}
