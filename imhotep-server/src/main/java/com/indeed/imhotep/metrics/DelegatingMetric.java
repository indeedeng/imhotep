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
 package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * A "pointer" to another metric farther down the stack.
 * @author dwahler
 */
public class DelegatingMetric implements IntValueLookup {
    private final IntValueLookup inner;

    public DelegatingMetric(IntValueLookup inner) {
        this.inner = inner;
    }

    @Override
    public long getMin() {
        return inner.getMin();
    }

    @Override
    public long getMax() {
        return inner.getMax();
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        inner.lookup(docIds, values, n);
    }

    @Override
    public long memoryUsed() {
        return 0;
    }

    @Override
    public void close() {
        // nothing
    }
}
