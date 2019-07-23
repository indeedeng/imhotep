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

import java.util.Arrays;

final class ConstantGroupLookup extends GroupLookup {
    private final int constant;
    private final int size;

    ConstantGroupLookup(final int constant, final int size) {
        this.constant = constant;
        this.size = size;
        this.numGroups = constant + 1;
    }

    public int getConstantGroup() {
        return constant;
    }

    @Override
    public int nextGroupCallback(final int n,
                                 final long[][] termGrpStats,
                                 final BitTree groupsSeen,
                                 final int[] docIdBuf,
                                 final long[] valBuf,
                                 final int[] docGroupBuffer,
                                 final ImhotepLocalSession.MetricStack metricStack) {
        if (constant == 0) {
            return 0;
        }

        if (n > 0) {
            groupsSeen.set(constant);
            if (metricStack.getNumStats() > 0) {
                Arrays.fill(docGroupBuffer, 0, n, constant);
                for (int statIndex = 0; statIndex < metricStack.getNumStats(); statIndex++) {
                    ImhotepJavaLocalSession.updateGroupStatsDocIdBuf(metricStack.get(statIndex), termGrpStats[statIndex], docGroupBuffer, docIdBuf, valBuf, n);
                }
            }
        }
        return n;
    }

    @Override
    public int get(final int doc) {
        return constant;
    }

    @Override
    public void set(final int doc, final int group) {
        throw new UnsupportedOperationException("bug!");
    }

    @Override
    public void batchSet(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        throw new UnsupportedOperationException("bug!");
    }

    @Override
    public void fill(final int group) {
        // no-op
    }

    @Override
    public GroupLookup makeCopy(final MemoryReservationContext memory) throws ImhotepOutOfMemoryException {
        // can't fail right now but who knows in the future?
        memory.claimMemoryOrThrowIOOME(memoryUsed());
        final ConstantGroupLookup constantGroupLookup = new ConstantGroupLookup(constant, size);
        constantGroupLookup.numGroups = numGroups;
        return constantGroupLookup;
    }

    @Override
    public void copyInto(final GroupLookup other) {
        other.fill(constant);
        other.numGroups = this.numGroups;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int maxGroup() {
        return constant;
    }

    @Override
    public long memoryUsed() {
        return 0L;
    }

    @Override
    public void fillDocGrpBuffer(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; ++i) {
            docGrpBuffer[i] = constant;
        }
    }

    @Override
    public void fillDocGrpBufferSequential(final int start, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; i++) {
            docGrpBuffer[i] = constant;
        }
    }

    @Override
    public void bitSetRegroup(
            final FastBitSet bitSet,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) {
        throw new UnsupportedOperationException("bug!");
    }

    @Override
    protected void recalculateNumGroups() {
        this.numGroups = constant + 1;
    }

    @Override
    public boolean canRepresentAllValuesUpToMaxGroup() {
        return constant == 0;
    }
}
