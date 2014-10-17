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
public abstract class AbstractBinaryOperator implements IntValueLookup {
    public static final int INITIAL_BUFFER_SIZE = 32;

    protected final IntValueLookup a;
    protected final IntValueLookup b;

    protected long[] buffer = new long[INITIAL_BUFFER_SIZE];

    protected AbstractBinaryOperator(IntValueLookup a, IntValueLookup b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public long getMin() {
        return Long.MIN_VALUE;
    }

    @Override
    public long getMax() {
        return Long.MAX_VALUE;
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        if (buffer.length < n) buffer = new long[n];
        a.lookup(docIds, values, n);
        b.lookup(docIds, buffer, n);
        combine(values, buffer, n);
    }

    protected abstract void combine(long[] values, long[] buffer, int n);

    @Override
    public long memoryUsed() {
        return a.memoryUsed() + b.memoryUsed();
    }

    @Override
    public void close() {
        a.close();
        b.close();
    }
}
