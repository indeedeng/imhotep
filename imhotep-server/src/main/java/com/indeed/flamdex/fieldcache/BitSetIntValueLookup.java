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
 package com.indeed.flamdex.fieldcache;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;

/**
 * @author jsgroth
 */
public final class BitSetIntValueLookup implements IntValueLookup {
    private FastBitSet lookupBitSet;

    public BitSetIntValueLookup(final FastBitSet lookupBitSet) {
        this.lookupBitSet = lookupBitSet;
    }

    @Override
    public long getMin() {
        return 0;
    }

    @Override
    public long getMax() {
        return 1;
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
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
