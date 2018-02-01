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
 * Fixed-point exponential function
 * @author dwahler
 * f(x) = scale * e^(x/scale)
 * f(x) is increasing function for any value of scale
 */
public class Exponential implements IntValueLookup {
    private final IntValueLookup operand;
    private final double scaleFactor;

    public Exponential(final IntValueLookup operand, final int scaleFactor) {
        this.operand = operand;
        this.scaleFactor = (double) scaleFactor;
    }

    @Override
    public long getMin() {
        return eval(operand.getMin(), scaleFactor);
    }

    @Override
    public long getMax() {
        return eval(operand.getMax(), scaleFactor);
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            values[i] = eval(values[i], scaleFactor);
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

    private static long eval(final long value, final double scale) {
        final double x = value / scale;
        final double result = Math.exp(x);

        // the output is clamped to [Integer.MIN_VALUE, Integer.MAX_VALUE] (JLS §5.1.3)
        return (long) (result * scale);
    }
}
