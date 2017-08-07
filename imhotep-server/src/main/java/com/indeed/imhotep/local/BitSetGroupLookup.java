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
 package com.indeed.imhotep.local;

import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.util.core.threads.ThreadSafeBitSet;

final class BitSetGroupLookup extends GroupLookup {
    /**
     *
     */
    private final ImhotepLocalSession session;
    private final FastBitSet bitSet;
    private final int size;

    BitSetGroupLookup(final ImhotepLocalSession imhotepLocalSession, final int size) {
        this.session = imhotepLocalSession;
        this.size = size;
        this.bitSet = new FastBitSet(size);
    }

    @Override
    public void nextGroupCallback(final int n, final long[][] termGrpStats, final BitTree groupsSeen) {
        int rewriteHead = 0;
        // remap groups and filter out useless docids (ones with group = 0), keep track of groups that were found
        for (int i = 0; i < n; i++) {
            final int docId = session.docIdBuf[i];
            if (!bitSet.get(docId)) {
                continue;
            }

            session.docGroupBuffer[rewriteHead] = 1;
            session.docIdBuf[rewriteHead] = docId;
            rewriteHead++;
        }
        groupsSeen.set(session.docGroupBuffer, rewriteHead);

        if (rewriteHead > 0) {
            for (int statIndex = 0; statIndex < session.numStats; statIndex++) {
                ImhotepJavaLocalSession.updateGroupStatsDocIdBuf(session.statLookup.get(statIndex), termGrpStats[statIndex], session.docGroupBuffer, session.docIdBuf, session.valBuf, rewriteHead);
            }
        }
    }

    @Override
    public void applyIntConditionsCallback(
            final int n,
            final ThreadSafeBitSet docRemapped,
            final GroupRemapRule[] remapRules,
            final String intField,
            final long itrTerm) {
        for (int i = 0; i < n; i++) {
            final int docId = session.docIdBuf[i];
            if (docRemapped.get(docId)) {
                continue;
            }
            final int group = bitSet.get(docId) ? 1 : 0;
            if (remapRules[group] == null) {
                continue;
            }
            if (ImhotepLocalSession.checkIntCondition(remapRules[group].condition, intField, itrTerm)) {
                continue;
            }
            bitSet.set(docId, remapRules[group].positiveGroup == 1);
            docRemapped.set(docId);
        }
    }

    @Override
    public void applyStringConditionsCallback(
            final int n,
            final ThreadSafeBitSet docRemapped,
            final GroupRemapRule[] remapRules,
            final String stringField,
            final String itrTerm) {
        for (int i = 0; i < n; i++) {
            final int docId = session.docIdBuf[i];
            if (docRemapped.get(docId)) {
                continue;
            }
            final int group = bitSet.get(docId) ? 1 : 0;
            if (remapRules[group] == null) {
                continue;
            }
            if (ImhotepLocalSession.checkStringCondition(remapRules[group].condition, stringField, itrTerm)) {
                continue;
            }
            bitSet.set(docId, remapRules[group].positiveGroup == 1);
            docRemapped.set(docId);
        }
    }

    @Override
    public int get(final int doc) {
        return bitSet.get(doc) ? 1 : 0;
    }

    @Override
    public void set(final int doc, final int group) {
        bitSet.set(doc, group == 1);
    }

    @Override
    public void batchSet(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; ++i) {
            bitSet.set(docIdBuf[i], docGrpBuffer[i] == 1);
        }
    }

    @Override
    public void fill(final int group) {
        if (group == 0) {
            bitSet.clearAll();
        } else if (group == 1) {
            bitSet.setAll();
        } else {
            throw new IllegalArgumentException("max allowed group is 1, was passed in "+group);
        }
    }

    @Override
    public void copyInto(final GroupLookup other) {
        if (size != other.size()) {
            throw new IllegalArgumentException("size does not match other.size: size="+size+", other.size="+other.size());
        }

        for (int i = 0; i < other.size(); ++i) {
            other.set(i, bitSet.get(i) ? 1 : 0);
        }
        other.numGroups = this.numGroups;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int maxGroup() {
        return 1;
    }

    @Override
    public long memoryUsed() {
        return bitSet.memoryUsage();
    }

    @Override
    public void fillDocGrpBuffer(final int[] docIdBuf, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; ++i) {
            docGrpBuffer[i] = bitSet.get(docIdBuf[i]) ? 1 : 0;
        }
    }

    @Override
    public void fillDocGrpBufferSequential(final int start, final int[] docGrpBuffer, final int n) {
        for (int i = 0; i < n; i++) {
            docGrpBuffer[i] = bitSet.get(start+i) ? 1 : 0;
        }
    }

    @Override
    public void bitSetRegroup(
            final FastBitSet bitSet,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) {
        // assuming targetGroup == 1 since nothing else would make sense
        if (negativeGroup == 0 && positiveGroup == 1) {
            this.bitSet.and(bitSet);
        } else {
            for (int doc = 0; doc < this.bitSet.size(); ++doc) {
                if (this.bitSet.get(doc)) {
                    this.bitSet.set(doc, bitSet.get(doc) ? positiveGroup == 1 : negativeGroup == 1);
                }
            }
        }
    }

    @Override
    protected void recalculateNumGroups() {
        for (int i = 0; i < bitSet.size(); ++i) {
            if (bitSet.get(i)) {
                this.numGroups = 2;
                return;
            }
        }
        this.numGroups = 1;
    }

    public static long calcMemUsageForSize(final int sz) {
        return 8L * ((sz + 64) >> 6);
    }

    @Override
    public ImhotepLocalSession getSession() {
        return this.session;
    }
}
