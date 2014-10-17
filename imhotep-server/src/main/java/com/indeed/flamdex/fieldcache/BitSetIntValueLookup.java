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
 package com.indeed.flamdex.fieldcache;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;

/**
 * @author jsgroth
 */
public final class BitSetIntValueLookup implements IntValueLookup {
    private FastBitSet lookupBitSet;
    private final long min;
    private final long max;

    public BitSetIntValueLookup(FastBitSet lookupBitSet) {
        this(lookupBitSet, 0, 1);
    }

    public BitSetIntValueLookup(FastBitSet lookupBitSet, long min, long max) {
        this.lookupBitSet = lookupBitSet;
        this.min = min;
        this.max = max;
    }

    @Override
    public long getMin() {
        return min;
    }

    @Override
    public long getMax() {
        return max;
    }

    @Override
    public final void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = lookupBitSet.get(docIds[i]) ? 1 : 0;
        }
    }

    @Override
    public long memoryUsed() {
        return lookupBitSet.memoryUsage();
    }

    @Override
    public void close() {
        lookupBitSet = null;
    }
}
