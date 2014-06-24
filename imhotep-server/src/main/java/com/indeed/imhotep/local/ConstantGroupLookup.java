package com.indeed.imhotep.local;

import com.indeed.util.core.threads.ThreadSafeBitSet;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.GroupRemapRule;

final class ConstantGroupLookup extends GroupLookup {
    /**
     *
     */
    private final ImhotepLocalSession session;
    private final int constant;
    private final int size;

    ConstantGroupLookup(ImhotepLocalSession imhotepLocalSession, int constant, int size) {
        session = imhotepLocalSession;
        this.constant = constant;
        this.size = size;
        this.numGroups = constant + 1;
    }

    @Override
    public void nextGroupCallback(int n, long[][] termGrpStats, BitTree groupsSeen) {
        int rewriteHead = 0;
        // remap groups and filter out useless docids (ones with group = 0), keep track of groups that were found
        for (int i = 0; i < n; i++) {
            final int docId = session.docIdBuf[i];

            session.docGroupBuffer[rewriteHead] = constant;
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
        throw new UnsupportedOperationException("bug!");
    }

    @Override
    public void applyStringConditionsCallback(int n, ThreadSafeBitSet docRemapped, GroupRemapRule[] remapRules, String stringField, String itrTerm) {
        throw new UnsupportedOperationException("bug!");
    }

    @Override
    public int get(int doc) {
        return constant;
    }

    @Override
    public void set(int doc, int group) {
        throw new UnsupportedOperationException("bug!");
    }

    @Override
    public void batchSet(int[] docIdBuf, int[] docGrpBuffer, int n) {
        throw new UnsupportedOperationException("bug!");
    }

    @Override
    public void fill(int group) {
        // no-op
    }

    @Override
    public void copyInto(GroupLookup other) {
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
    public void fillDocGrpBuffer(int[] docIdBuf, int[] docGrpBuffer, int n) {
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
    public void bitSetRegroup(FastBitSet bitSet, int targetGroup, int negativeGroup, int positiveGroup) {
        throw new UnsupportedOperationException("bug!");
    }

    @Override
    protected void recalculateNumGroups() {
        this.numGroups = constant + 1;
        return;
    }

    @Override
    public ImhotepLocalSession getSession() {
        return this.session;
    }
}