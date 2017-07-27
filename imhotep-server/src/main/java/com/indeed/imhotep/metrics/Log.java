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
 * User: arun
 * Note: Since the result is stored as an int, Log(0) will be INTEGER.MIN_VALUE.(JLS §5.1.3)
 */
public final class Log implements IntValueLookup {

    private final IntValueLookup operand;
    private final int scaleFactor;
    private final double logScaleFactor;

    public Log(final IntValueLookup operand, final int scaleFactor) {
        this.operand = operand;
        this.scaleFactor = scaleFactor;
        this.logScaleFactor = Math.log(scaleFactor);
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
    public void lookup(final int[] docIds, final long[] values, final int n) {
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            values[i] = (long) ((Math.log(values[i]) - logScaleFactor) * scaleFactor);
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
