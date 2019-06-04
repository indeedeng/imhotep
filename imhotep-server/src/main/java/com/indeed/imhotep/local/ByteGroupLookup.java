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

final class ByteGroupLookup extends GroupLookup implements ArrayBasedGroupLookup {
    private final byte[] docIdToGroup;

    ByteGroupLookup(final int size) {
        docIdToGroup = new byte[size];
    }

    private ByteGroupLookup(final byte[] docIdToGroup) {
        this.docIdToGroup = docIdToGroup;
    }

    byte[] getDocIdToGroup() { return docIdToGroup; }

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
            final int group = docIdToGroup[docId] & 0xFF;
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
        return docIdToGroup[doc] & 0xFF;
    }

    @Override
    public void set(final int doc, final int group) {
        docIdToGroup[doc] = (byte)group;
    }

    @Override
    public void batchSet(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; ++i) {
            docIdToGroup[docIdBuf[i]] = (byte)docGrpBuffer[i];
        }
    }

    @Override
    public void fill(final int group) {
        if (group > 255) {
            throw new IllegalArgumentException("group is too big: max=255, group="+group);
        }

        Arrays.fill(docIdToGroup, (byte)group);
    }

    @Override
    public GroupLookup makeCopy(final MemoryReservationContext memory) throws ImhotepOutOfMemoryException {
        if (!memory.claimMemory(memoryUsed())) {
            throw new ImhotepOutOfMemoryException();
        }
        final ByteGroupLookup byteGroupLookup = new ByteGroupLookup(Arrays.copyOf(docIdToGroup, docIdToGroup.length));
        byteGroupLookup.numGroups = numGroups;
        return byteGroupLookup;
    }

    @Override
    public void copyInto(final GroupLookup other) {
        if (docIdToGroup.length != other.size()) {
            throw new IllegalArgumentException("sizes don't match: size="+docIdToGroup.length+", other.size="+other.size());
        }

        for (int i = 0; i < other.size(); ++i) {
            other.set(i, docIdToGroup[i] & 0xFF);
        }
        other.numGroups = this.numGroups;
    }

    @Override
    public int size() {
        return docIdToGroup.length;
    }

    @Override
    public int maxGroup() {
        return 255;
    }

    @Override
    public long memoryUsed() {
        return docIdToGroup.length;
    }

    @Override
    public void fillDocGrpBuffer(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; ++i) {
            docGrpBuffer[i] = docIdToGroup[docIdBuf[i]] & 0xFF;
        }
    }

    @Override
    public void fillDocGrpBufferSequential(final int start, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; i++) {
            docGrpBuffer[i] = docIdToGroup[start+i] & 0xFF;
        }
    }

    @Override
    public void bitSetRegroup(
            final FastBitSet bitSet,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) {
        for (int i = 0; i < docIdToGroup.length; ++i) {
            final int group = docIdToGroup[i] & 0xFF;
            if (group == targetGroup) {
                docIdToGroup[i] = (byte) (bitSet.get(i) ? positiveGroup : negativeGroup);
            }
        }
    }

    @Override
    protected void recalculateNumGroups() {
        int max = 0;
        for (final byte group : docIdToGroup) {
            max = Math.max(max, (group & 0xFF) + 1);
        }
        this.numGroups = max;
    }

    @Override
    public boolean canRepresentAllValuesUpToMaxGroup() {
        return true;
    }

    public static long calcMemUsageForSize(final int sz) {
        return sz;
    }
}
