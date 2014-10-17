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

import com.indeed.util.core.threads.ThreadSafeBitSet;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.GroupRemapRule;

abstract class GroupLookup {
    protected int numGroups;

    // returns new value for groupsSeenCount
    abstract void nextGroupCallback(int n, long[][] termGrpStats, BitTree groupsSeen);
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