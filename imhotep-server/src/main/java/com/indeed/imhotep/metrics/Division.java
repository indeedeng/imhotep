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
 * @author jsgroth
 */
public class Division extends AbstractBinaryOperator {
    public Division(final IntValueLookup a, final IntValueLookup b) {
        super(a, b);
    }

    @Override
    public long getMin() {
        final long maxB = b.getMax();
        final long minB = b.getMin();
        long result = Long.MAX_VALUE;
        if (maxB > 0) {
            // at least part of B range is positive
            final long minPositive = Math.max(minB, 1);
            // A / [minPositive..maxB]
            // min is one of minA/minPositive and minA/maxB
            final long minA = a.getMin();
            result = Math.min(result,
                    Math.min(eval(minA, minPositive), eval(minA, maxB)));
        }

        if (minB < 0) {
            // at least part of B range is negative
            final long maxNegative = Math.min(maxB, -1);
            // A / [minB..maxNegative] == [-maxA..-minA] / [-maxNegative..-minB] where divider is positive
            // min is one of (-maxA)/(-maxNegative) and (-maxA)/(-minB), we can omit negate signs.
            final long maxA = a.getMax();
            result = Math.min(result,
                    Math.min(eval(maxA, maxNegative), eval(maxA, minB)));
        }

        // check if zero is in B range and update result if so.
        if (maxB >= 0 && minB <= 0) {
            result = Math.min(result, 0);
        }
        return result;
    }

    @Override
    public long getMax() {
        final long maxB = b.getMax();
        final long minB = b.getMin();
        long result = Long.MIN_VALUE;
        if (maxB > 0) {
            // at least part of B range is positive
            final long minPositive = Math.max(minB, 1);
            // A / [minPositive..maxB]
            // max is one of maxA/minPositive and maxA/maxB
            final long maxA = a.getMax();
            result = Math.max(result,
                    Math.max(eval(maxA, minPositive), eval(maxA, maxB)));
        }

        if (minB < 0) {
            // at least part of B range is negative
            final long maxNegative = Math.min(maxB, -1);
            // A / [minB..maxNegative] == [-maxA..-minA] / [-maxNegative..-minB] where divider is positive
            // max is one of (-minA)/(-maxNegative) and (-minA)/(-minB), we can omit negate signs.
            final long minA = a.getMin();
            result = Math.max(result,
                    Math.max(eval(minA, maxNegative), eval(minA, minB)));
        }

        // check if zero is in B range and update result if so.
        if (maxB >= 0 && minB <= 0) {
            result = Math.max(result, 0);
        }
        return result;
    }

    @Override
    protected void combine(final long[] values, final long[] buffer, final int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = eval(values[i], buffer[i]);
        }
    }

    private long eval(final long a, final long b) {
        return b != 0 ? a / b : 0;
    }
}
