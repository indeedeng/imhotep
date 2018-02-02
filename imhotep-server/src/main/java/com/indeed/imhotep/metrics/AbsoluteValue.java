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
 * Absolute value function
 * @author jwolfe
 */
public class AbsoluteValue implements IntValueLookup {
    private final IntValueLookup operand;

    public AbsoluteValue(final IntValueLookup operand) {
        this.operand = operand;
    }

    @Override
    public long getMin() {
        final long max = operand.getMax();
        if (max <= 0) {
            // min..max..zero -> zero..-max..-min
            return -max;
        }

        final long min = operand.getMin();

        if (min >= 0) {
            // zero..min..max -> zero..min..max
            return min;
        }

        // min..zero..max, min abs value is zero
        return 0;
    }

    @Override
    public long getMax() {
        final long max = operand.getMin();
        final long min = operand.getMin();
        if (max <= 0) {
            // min..max..zero -> zero..-max..-min
            return -min;
        }

        if (min >= 0) {
            // zero..min..max -> zero..min..max
            return max;
        }

        // min..zero..max
        return Math.max(-min, max);
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            values[i] = Math.abs(values[i]);
        }
    }

    @Override
    public long memoryUsed() {
        return operand.memoryUsed();
    }

    @Override
    public void close() {
        operand.close();
    }
}
