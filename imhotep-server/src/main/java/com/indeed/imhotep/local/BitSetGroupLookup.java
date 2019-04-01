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

import com.google.common.base.Preconditions;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.util.core.threads.ThreadSafeBitSet;

import java.util.Arrays;

final class BitSetGroupLookup extends GroupLookup {
    private final FastBitSet bitSet;
    private final int size;

    private int nonZeroGroup;

    BitSetGroupLookup(final int size) {
        this(size, 1);
    }

    BitSetGroupLookup(final int size, final int nonZeroGroup) {
        Preconditions.checkArgument(nonZeroGroup > 0, "nonZeroGroup must be positive");
        this.size = size;
        this.bitSet = new FastBitSet(size);
        this.nonZeroGroup = nonZeroGroup;
    }

    int getNonZeroGroup() {
        return nonZeroGroup;
    }

    void setNonZeroGroup(final int nonZeroGroup) {
        Preconditions.checkArgument(nonZeroGroup > 0, "nonZeroGroup must be positive");
        this.nonZeroGroup = nonZeroGroup;
    }

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
            if (!bitSet.get(docId)) {
                continue;
            }

            docIdBuf[rewriteHead] = docId;
            rewriteHead++;
        }

        if (rewriteHead > 0) {
            groupsSeen.set(nonZeroGroup);
            if (metricStack.getNumStats() > 0) {
                Arrays.fill(docGroupBuffer, 0, rewriteHead, nonZeroGroup);
                for (int statIndex = 0; statIndex < metricStack.getNumStats(); statIndex++) {
                    ImhotepJavaLocalSession.updateGroupStatsDocIdBuf(metricStack.get(statIndex), termGrpStats[statIndex], docGroupBuffer, docIdBuf, valBuf, rewriteHead);
                }
            }
        }

        return rewriteHead;
    }

    @Override
    public void applyIntConditionsCallback(
            final int n,
            final int[] docIdBuf,
            final ThreadSafeBitSet docRemapped,
            final GroupRemapRule[] remapRules,
            final String intField,
            final long itrTerm) {
        for (int i = 0; i < n; i++) {
            final int docId = docIdBuf[i];
            if (docRemapped.get(docId)) {
                continue;
            }
            final int group = bitSet.get(docId) ? nonZeroGroup : 0;
            if (remapRules[group] == null) {
                continue;
            }
            if (ImhotepLocalSession.checkIntCondition(remapRules[group].condition, intField, itrTerm)) {
                continue;
            }
            set(docId, remapRules[group].positiveGroup);
            docRemapped.set(docId);
        }
    }

    @Override
    public void applyStringConditionsCallback(
            final int n,
            final int[] docIdBuf,
            final ThreadSafeBitSet docRemapped,
            final GroupRemapRule[] remapRules,
            final String stringField,
            final String itrTerm) {
        for (int i = 0; i < n; i++) {
            final int docId = docIdBuf[i];
            if (docRemapped.get(docId)) {
                continue;
            }
            final int group = bitSet.get(docId) ? nonZeroGroup : 0;
            if (remapRules[group] == null) {
                continue;
            }
            if (ImhotepLocalSession.checkStringCondition(remapRules[group].condition, stringField, itrTerm)) {
                continue;
            }
            set(docId, remapRules[group].positiveGroup);
            docRemapped.set(docId);
        }
    }

    @Override
    public int get(final int doc) {
        return bitSet.get(doc) ? nonZeroGroup : 0;
    }

    @Override
    public void set(final int doc, final int group) {
        Preconditions.checkArgument((group == 0) || (group == nonZeroGroup), "group must be in {0, nonZeroGroup}");
        bitSet.set(doc, group == nonZeroGroup);
    }

    @Override
    public void batchSet(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; ++i) {
            final int group = docGrpBuffer[i];
            Preconditions.checkArgument((group == 0) || (group == nonZeroGroup), "group must be in {0, nonZeroGroup}");
            bitSet.set(docIdBuf[i], group == nonZeroGroup);
        }
    }

    @Override
    public void fill(final int group) {
        if (group == 0) {
            bitSet.clearAll();
        } else if (group == nonZeroGroup) {
            bitSet.setAll();
        } else {
            throw new IllegalArgumentException("only groups in {0, " + nonZeroGroup + "} allowed. Was passed " + group);
        }
    }

    @Override
    public void copyInto(final GroupLookup other) {
        if (size != other.size()) {
            throw new IllegalArgumentException("size does not match other.size: size="+size+", other.size="+other.size());
        }

        for (int i = 0; i < other.size(); ++i) {
            other.set(i, bitSet.get(i) ? nonZeroGroup : 0);
        }
        other.numGroups = this.numGroups;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int maxGroup() {
        return nonZeroGroup;
    }

    @Override
    public long memoryUsed() {
        return bitSet.memoryUsage();
    }

    @Override
    public void fillDocGrpBuffer(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; ++i) {
            docGrpBuffer[i] = bitSet.get(docIdBuf[i]) ? nonZeroGroup : 0;
        }
    }

    @Override
    public void fillDocGrpBufferSequential(final int start, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; i++) {
            docGrpBuffer[i] = bitSet.get(start+i) ? nonZeroGroup : 0;
        }
    }

    @Override
    public void bitSetRegroup(
            final FastBitSet bitSet,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) {
        Preconditions.checkArgument((negativeGroup == 0) || (negativeGroup == nonZeroGroup), "negativeGroup must be in {0, nonZeroGroup}");
        Preconditions.checkArgument((positiveGroup == 0) || (positiveGroup == nonZeroGroup), "positiveGroup must be in {0, nonZeroGroup}");
        if (targetGroup != nonZeroGroup) {
            // No work to do!
            return;
        }
        // Can now assume targetGroup == nonZeroGroup
        if ((negativeGroup == 0) && (positiveGroup == nonZeroGroup)) {
            this.bitSet.and(bitSet);
        } else {
            for (int doc = 0; doc < this.bitSet.size(); ++doc) {
                if (this.bitSet.get(doc)) {
                    this.bitSet.set(doc, bitSet.get(doc) ? (positiveGroup == nonZeroGroup) : (negativeGroup == nonZeroGroup));
                }
            }
        }
    }

    @Override
    protected void recalculateNumGroups() {
        for (int i = 0; i < bitSet.size(); ++i) {
            if (bitSet.get(i)) {
                this.numGroups = nonZeroGroup + 1;
                return;
            }
        }
        this.numGroups = 1;
    }

    @Override
    public boolean canRepresentAllValuesUpToMaxGroup() {
        return nonZeroGroup == 1;
    }

    public static long calcMemUsageForSize(final int sz) {
        return 8L * ((sz + 64) >> 6);
    }
}
