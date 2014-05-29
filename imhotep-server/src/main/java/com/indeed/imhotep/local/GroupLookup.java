package com.indeed.imhotep.local;

import com.indeed.util.core.threads.ThreadSafeBitSet;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.GroupRemapRule;

abstract class GroupLookup {
    protected int numGroups;
    
    // returns new value for groupsSeenCount
    abstract int nextGroupCallback(int n, long[] termGrpCounts, long[][] termGrpStats, int[] groupsSeen, int groupsSeenCount);
    abstract void applyIntConditionsCallback(int n, ThreadSafeBitSet docRemapped, GroupRemapRule[] remapRules, String intField, long itrTerm);
    abstract void applyStringConditionsCallback(int n, ThreadSafeBitSet docRemapped, GroupRemapRule[] remapRules, String stringField, String itrTerm);
    abstract int get(int doc);
    abstract void set(int doc, int group);
    abstract void batchSet(int[] docIdBuf, int[] docGrpBuffer, int n);
    abstract void fill(int group);
    abstract void copyInto(GroupLookup other);
    abstract int size();
    abstract int maxGroup();
    abstract long memoryUsed();
    abstract void fillDocGrpBuffer(int[] docIdBuf, int[] docGrpBuffer, int n);
    abstract void fillDocGrpBufferSequential(int start, int[] docGrpBuffer, int n);
    abstract void bitSetRegroup(FastBitSet bitSet, int targetGroup, int negativeGroup, int positiveGroup);
    abstract ImhotepLocalSession getSession();
    abstract void recalculateNumGroups();

    final int getNumGroups() {
        return numGroups;
    }
}