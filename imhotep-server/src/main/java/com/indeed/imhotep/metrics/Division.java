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
        if (b.getMin() >= 0) {
            return Math.min(eval(a.getMin(), b.getMin()), eval(a.getMin(), b.getMax()));
        }
        else if (b.getMax() <= 0) {
            return Math.max(eval(a.getMax(), b.getMax()), eval(a.getMax(), b.getMin()));
        }
        else {
            return Math.min(-Math.abs(a.getMax()), -Math.abs(a.getMin()));
        }
    }

    @Override
    public long getMax() {
        if (b.getMin() >= 0) {
            return Math.max(eval(a.getMax(), b.getMax()), eval(a.getMax(), b.getMin()));
        }
        else if (b.getMax() <= 0) {
            return Math.min(eval(a.getMin(), b.getMin()), eval(a.getMin(), b.getMax()));
        }
        else {
            return Math.max(Math.abs(a.getMax()), Math.abs(a.getMin()));
        }
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
