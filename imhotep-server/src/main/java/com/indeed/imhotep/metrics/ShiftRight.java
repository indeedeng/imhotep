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
import org.apache.log4j.Logger;

/**
 * @author johnf
 */
public final class ShiftRight implements IntValueLookup {
    private static final Logger log = Logger.getLogger(ShiftRight.class);
    private final IntValueLookup operand;
    private final int shift;

    public ShiftRight(final IntValueLookup operand, final int shift) {
        this.operand = operand;
        this.shift = shift;
    }

    @Override
    public long getMin() {
        return this.operand.getMin() >> this.shift;
    }

    @Override
    public long getMax() {
        return this.operand.getMax() >> this.shift;
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
        operand.lookup(docIds, values, n);
        for (int i = 0; i < n; i++) {
            values[i] >>= this.shift;
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
