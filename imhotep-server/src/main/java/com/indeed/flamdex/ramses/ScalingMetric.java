/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.flamdex.ramses;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * @author jsgroth
 */
public class ScalingMetric implements IntValueLookup {
    private final IntValueLookup metric;
    private final int scaleFactor;

    public ScalingMetric(final IntValueLookup metric, final int scaleFactor) {
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
    public void lookup(final int[] docIds, final long[] values, final int n) {
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
