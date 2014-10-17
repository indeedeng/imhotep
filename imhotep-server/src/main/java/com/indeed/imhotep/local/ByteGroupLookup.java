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

import java.util.Arrays;

import com.indeed.util.core.threads.ThreadSafeBitSet;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.GroupRemapRule;

final class ByteGroupLookup extends GroupLookup {
    /**
     *
     */
    private final ImhotepLocalSession session;
    private final byte[] docIdToGroup;

    ByteGroupLookup(ImhotepLocalSession imhotepLocalSession, final int size) {
        session = imhotepLocalSession;
        docIdToGroup = new byte[size];
    }

    @Override
    public void nextGroupCallback(int n, long[][] termGrpStats, BitTree groupsSeen) {
        int rewriteHead = 0;
        // remap groups and filter out useless docids (ones with group = 0), keep track of groups that were found
        for (int i = 0; i < n; i++) {
            final int docId = session.docIdBuf[i];
            final int group = docIdToGroup[docId] & 0xFF;
            if (group == 0) continue;

            session.docGroupBuffer[rewriteHead] = group;
            session.docIdBuf[rewriteHead] = docId;
            rewriteHead++;
        }
        groupsSeen.set(session.docGroupBuffer, rewriteHead);

        if (rewriteHead > 0) {
            for (int statIndex = 0; statIndex < session.numStats; statIndex++) {
                ImhotepLocalSession.updateGroupStatsDocIdBuf(session.statLookup[statIndex], termGrpStats[statIndex], session.docGroupBuffer, session.docIdBuf, session.valBuf, rewriteHead);
            }
        }
    }

    @Override
    public void applyIntConditionsCallback(int n, ThreadSafeBitSet docRemapped, GroupRemapRule[] remapRules, String intField, long itrTerm) {
        for (int i = 0; i < n; i++) {
            final int docId = session.docIdBuf[i];
            if (docRemapped.get(docId)) continue;
            final int group = docIdToGroup[docId] & 0xFF;
            if (remapRules[group] == null) continue;
            if (ImhotepLocalSession.checkIntCondition(remapRules[group].condition, intField, itrTerm)) continue;
            docIdToGroup[docId] = (byte)remapRules[group].positiveGroup;
            docRemapped.set(docId);
        }
    }

    @Override
    public void applyStringConditionsCallback(int n, ThreadSafeBitSet docRemapped, GroupRemapRule[] remapRules, String stringField, String itrTerm) {
        for (int i = 0; i < n; i++) {
            final int docId = session.docIdBuf[i];
            if (docRemapped.get(docId)) continue;
            final int group = docIdToGroup[docId] & 0xFF;
            if (remapRules[group] == null) continue;
            if (ImhotepLocalSession.checkStringCondition(remapRules[group].condition, stringField, itrTerm)) continue;
            docIdToGroup[docId] = (byte)remapRules[group].positiveGroup;
            docRemapped.set(docId);
        }
    }

    @Override
    public int get(int doc) {
        return docIdToGroup[doc] & 0xFF;
    }

    @Override
    public void set(int doc, int group) {
        docIdToGroup[doc] = (byte)group;
    }

    @Override
    public void batchSet(int[] docIdBuf, int[] docGrpBuffer, int n) {
        for (int i = 0; i < n; ++i) {
            docIdToGroup[docIdBuf[i]] = (byte)docGrpBuffer[i];
        }
    }

    @Override
    public void fill(int group) {
        if (group > 255) {
            throw new IllegalArgumentException("group is too big: max=255, group="+group);
        }

        Arrays.fill(docIdToGroup, (byte)group);
    }

    @Override
    public void copyInto(GroupLookup other) {
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
    public void fillDocGrpBuffer(int[] docIdBuf, int[] docGrpBuffer, int n) {
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
    public void bitSetRegroup(FastBitSet bitSet, int targetGroup, int negativeGroup, int positiveGroup) {
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
        return;
    }

    public static long calcMemUsageForSize(int sz) {
        return sz;
    }

    @Override
    public ImhotepLocalSession getSession() {
        return this.session;
    }
}