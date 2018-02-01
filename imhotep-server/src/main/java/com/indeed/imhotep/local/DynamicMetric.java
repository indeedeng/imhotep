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
 package com.indeed.imhotep.local;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.indeed.flamdex.api.IntValueLookup;

import java.io.Serializable;

@VisibleForTesting
public class DynamicMetric implements IntValueLookup, Serializable {
    private static final long serialVersionUID = 1L;
    private final int[] values;
    private Integer min;
    private Integer max;

    public DynamicMetric(final int size) {
        this.values = new int[size];
    }

    @Override
    public long getMin() {
        if (min == null) {
            min = Ints.min(values);
        }
        return min;
    }

    @Override
    public long getMax() {
        if (max == null) {
            max = Ints.max(values);
        }
        return max;
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
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

    // don't forget to call resetMinMax!
    public void add(final int doc, final int delta) {
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

    // don't forget to call resetMinMax!
    public void set(final int doc, final int value) {
        values[doc] = value;
    }

    public int lookupSingleVal(final int docId) {
        return this.values[docId];
    }

    public void resetMinMax() {
        min = null;
        max = null;
    }
}