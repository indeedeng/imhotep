/*
 * Copyright (C) 2018 Indeed Inc.
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
 * f(x) = scale * log(1 + e^(x / scale))
 * f(x) is increasing function for any scale
 * @author jwolfe
 */
public class Log1pExp implements IntValueLookup {
    private final IntValueLookup operand;
    private final double scaleFactor;

    public Log1pExp(final IntValueLookup operand, final int scaleFactor) {
        this.operand = operand;
        this.scaleFactor = (double) scaleFactor;
    }

    @Override
    public long getMin() {
        return eval(operand.getMin());
    }

    @Override
    public long getMax() {
        return eval(operand.getMax());
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            values[i] = eval(values[i]);
        }
    }

    private long eval(final long value) {
        final double x = value / scaleFactor;
        final double result;
        if (x > 0) {
            // This is mathematically the same as log(1 + e^x):
            // log(1+e^x) = log(e^x * (e^(-x) + 1)) = log(e^x) + log(1 + e^-x) = x + log1p(e^-x)
            // Except it won't overflow the 64-bit floating point numbers with large values of x.
            result = x + Math.log1p(Math.exp(-x));
        } else {
            result = Math.log1p(Math.exp(x));
        }
        // the output is clamped to [Integer.MIN_VALUE, Integer.MAX_VALUE] (JLS §5.1.3)
        return (long) (result * scaleFactor);
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
