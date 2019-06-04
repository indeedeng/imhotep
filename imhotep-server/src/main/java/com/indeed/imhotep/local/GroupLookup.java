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
package com.indeed.imhotep.local;

import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

public abstract class GroupLookup {
    protected int numGroups;

    // returns how many non-zero group docs were processed by this call.
    public abstract int nextGroupCallback(
            int n,
            long[][] termGrpStats,
            BitTree groupsSeen,
            int[] docIdBuf,
            long[] valBuf,
            int[] docGroupBuffer,
            final ImhotepLocalSession.MetricStack metricStack);

    public abstract int get(int doc);
    public abstract void set(int doc, int group);
    public abstract void batchSet(int[] docIdBuf, int[] docGrpBuffer, int n);
    public abstract void fill(int group);
    public abstract GroupLookup makeCopy(final MemoryReservationContext memory) throws ImhotepOutOfMemoryException;
    public abstract void copyInto(GroupLookup other);
    public abstract int size();
    public abstract int maxGroup();
    public abstract long memoryUsed();
    public abstract void fillDocGrpBuffer(int[] docIdBuf, int[] docGrpBuffer, int n);
    public abstract void fillDocGrpBufferSequential(int start, int[] docGrpBuffer, int n);
    public abstract void bitSetRegroup(FastBitSet bitSet, int targetGroup, int negativeGroup, int positiveGroup);
    protected abstract void recalculateNumGroups();
    // returns true iff it's valid to call set(doc, N) for all values from 0 to maxGroup()
    public abstract boolean canRepresentAllValuesUpToMaxGroup();

    // If this definition changes, please see ImhotepLocalSession::weakGetNumGroups
    public final int getNumGroups() {
        return numGroups;
    }

    public final boolean isFilteredOut() {
        return numGroups == 1;
    }

    public int countGroupZeroDocs() {
        final int numDocs = size();
        int zeroCount = 0;
        for (int i = 0; i < numDocs; i++) {
            if (get(i) == 0) {
                zeroCount += 1;
            }
        }
        return zeroCount;
    }
}
