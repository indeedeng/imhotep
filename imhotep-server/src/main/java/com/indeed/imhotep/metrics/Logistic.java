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
 * f(x) = scaleUp / (1 + e^(-x / scaleDown))
 * f(x) is monotonic but if it's increasing or decreasing depends on scaleUp and scaleDown
 * @author jplaisance
 */
public final class Logistic implements IntValueLookup {
    private final IntValueLookup operand;
    private final double scaleDown;
    private final double scaleUp;

    public Logistic(final IntValueLookup operand, final double scaleDown, final double scaleUp) {
        this.operand = operand;
        this.scaleDown = scaleDown;
        this.scaleUp = scaleUp;
    }

    @Override
    public long getMin() {
        // since f(x) is monotonic min is on range border.
        return Math.min(eval(operand.getMin()), eval(operand.getMax()));
    }

    @Override
    public long getMax() {
        // since f(x) is monotonic max is on range border.
        return Math.max(eval(operand.getMin()), eval(operand.getMax()));
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            values[i] = eval(values[i]);
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

    private long eval(final long value) {
        final double x = value / scaleDown;
        return (long)(scaleUp/(1+Math.exp(-x)));
    }
}
