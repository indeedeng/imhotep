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

import java.util.Arrays;

final class ConstantGroupLookup extends GroupLookup {
    /**
     *
     */
    private final ImhotepLocalSession session;
    private final int constant;
    private final int size;

    ConstantGroupLookup(final ImhotepLocalSession imhotepLocalSession, final int constant, final int size) {
        session = imhotepLocalSession;
        this.constant = constant;
        this.size = size;
        this.numGroups = constant + 1;
    }

    public int getConstantGroup() {
        return constant;
    }

    @Override
    public int nextGroupCallback(final int n, final long[][] termGrpStats, final BitTree groupsSeen) {
        if (constant == 0) {
            return 0;
        }

        if (n > 0) {
            groupsSeen.set(constant);
            if (session.numStats > 0) {
                Arrays.fill(session.docGroupBuffer, 0, n, constant);
                for (int statIndex = 0; statIndex < session.numStats; statIndex++) {
                    ImhotepJavaLocalSession.updateGroupStatsDocIdBuf(session.statLookup.get(statIndex), termGrpStats[statIndex], session.docGroupBuffer, session.docIdBuf, session.valBuf, n);
                }
            }
        }
        return n;
    }

    @Override
    public void applyIntConditionsCallback(
            final int n,
            final ThreadSafeBitSet docRemapped,
            final GroupRemapRule[] remapRules,
            final String intField,
            final long itrTerm) {
        throw new UnsupportedOperationException("bug!");
    }

    @Override
    public void applyStringConditionsCallback(
            final int n,
            final ThreadSafeBitSet docRemapped,
            final GroupRemapRule[] remapRules,
            final String stringField,
            final String itrTerm) {
        throw new UnsupportedOperationException("bug!");
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
        return -1; // always trigger a new lookup allocation on regroups
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
    public ImhotepLocalSession getSession() {
        return this.session;
    }
}
