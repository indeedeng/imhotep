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

    public AbsoluteValue(IntValueLookup operand) {
        this.operand = operand;
    }

    @Override
    public long getMin() {
        return Math.min(Math.abs(operand.getMin()), Math.abs(operand.getMax()));
    }

    @Override
    public long getMax() {
        return Math.max(Math.abs(operand.getMin()), Math.abs(operand.getMax()));
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
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
