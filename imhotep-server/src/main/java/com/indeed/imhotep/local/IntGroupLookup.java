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

final class IntGroupLookup extends GroupLookup implements ArrayBasedGroupLookup {
    private final int[] docIdToGroup;

    IntGroupLookup(final int size) {
        docIdToGroup = new int[size];
    }

    IntGroupLookup(final int[] content) {
        docIdToGroup = content;
    }

    int[] getDocIdToGroup() { return docIdToGroup; }

    @Override
    public int nextGroupCallback(final int n,
                                 final long[][] termGrpStats,
                                 final BitTree groupsSeen,
                                 final int[] docIdBuf,
                                 final long[] valBuf,
                                 final int[] docGroupBuffer,
                                 final ImhotepLocalSession.MetricStack metricStack) {
        int rewriteHead = 0;
        // remap groups and filter out useless docids (ones with group = 0), keep track of groups that were found
        for (int i = 0; i < n; i++) {
            final int docId = docIdBuf[i];
            final int group = docIdToGroup[docId];
            if (group == 0) {
                continue;
            }

            docGroupBuffer[rewriteHead] = group;
            docIdBuf[rewriteHead] = docId;
            rewriteHead++;
        }
        groupsSeen.set(docGroupBuffer, rewriteHead);

        if (rewriteHead > 0) {
            for (int statIndex = 0; statIndex < metricStack.getNumStats(); statIndex++) {
                ImhotepJavaLocalSession.updateGroupStatsDocIdBuf(metricStack.get(statIndex), termGrpStats[statIndex], docGroupBuffer, docIdBuf, valBuf, rewriteHead);
            }
        }

        return rewriteHead;
    }

    @Override
    public int get(final int doc) {
        return docIdToGroup[doc];
    }

    @Override
    public void set(final int doc, final int group) {
        docIdToGroup[doc] = group;
    }

    @Override
    public void batchSet(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; ++i) {
            docIdToGroup[docIdBuf[i]] = docGrpBuffer[i];
        }
    }

    @Override
    public void fill(final int group) {
        Arrays.fill(docIdToGroup, group);
    }

    @Override
    public GroupLookup makeCopy(final MemoryReservationContext memory) throws ImhotepOutOfMemoryException {
        if (!memory.claimMemory(memoryUsed())) {
            throw new ImhotepOutOfMemoryException();
        }
        final IntGroupLookup intGroupLookup = new IntGroupLookup(Arrays.copyOf(docIdToGroup, docIdToGroup.length));
        intGroupLookup.numGroups = numGroups;
        return intGroupLookup;
    }

    @Override
    public void copyInto(final GroupLookup other) {
        if (docIdToGroup.length != other.size()) {
            throw new IllegalArgumentException("size != other.size: size="+docIdToGroup.length+", other.size="+other.size());
        }

        for (int i = 0; i < docIdToGroup.length; ++i) {
            other.set(i, docIdToGroup[i]);
        }
        other.numGroups = this.numGroups;
    }

    @Override
    public int size() {
        return docIdToGroup.length;
    }

    @Override
    public int maxGroup() {
        return Integer.MAX_VALUE;
    }

    @Override
    public long memoryUsed() {
        return 4L*docIdToGroup.length;
    }

    @Override
    public void fillDocGrpBuffer(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; ++i) {
            docGrpBuffer[i] = docIdToGroup[docIdBuf[i]];
        }
    }

    @Override
    public void fillDocGrpBufferSequential(final int start, final int[] docGrpBuffer, final int n) {
        System.arraycopy(docIdToGroup, start, docGrpBuffer, 0, n);
    }

    @Override
    public void bitSetRegroup(
            final FastBitSet bitSet,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) {
        for (int i = 0; i < docIdToGroup.length; ++i) {
            if (docIdToGroup[i] == targetGroup) {
                docIdToGroup[i] = bitSet.get(i) ? positiveGroup : negativeGroup;
            }
        }
    }

    @Override
    protected void recalculateNumGroups() {
        int max = 0;
        for (final int group : docIdToGroup) {
            max = Math.max(max, group + 1);
        }
        this.numGroups = max;
    }

    @Override
    public boolean canRepresentAllValuesUpToMaxGroup() {
        return true;
    }

    public static long calcMemUsageForSize(final int sz) {
        return sz * 4;
    }
}
