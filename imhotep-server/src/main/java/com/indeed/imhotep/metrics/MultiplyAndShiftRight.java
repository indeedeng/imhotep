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
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class MultiplyAndShiftRight extends AbstractBinaryOperator {
    private static final Logger log = Logger.getLogger(MultiplyAndShiftRight.class);
    private final int shift;

    public MultiplyAndShiftRight(final IntValueLookup a, final IntValueLookup b, int shift) {
        super(a, b);
        this.shift = shift;
    }

    protected void combine(final long[] values, final long[] buffer, final int n) {
        for (int i = 0; i < n; i++) {
            values[i] = eval(values[i], buffer[i]);
        }
    }

    private long eval(long a, long b) {
        final long result = a * b;
        return result >> shift;
    }

}
