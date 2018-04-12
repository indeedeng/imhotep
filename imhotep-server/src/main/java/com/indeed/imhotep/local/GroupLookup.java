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
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.util.core.threads.ThreadSafeBitSet;

public abstract class GroupLookup {
    protected int numGroups;

    // returns how many non-zero group docs were processed by this call.
    public abstract int nextGroupCallback(int n, long[][] termGrpStats, BitTree groupsSeen);

    public abstract void applyIntConditionsCallback(int n, ThreadSafeBitSet docRemapped, GroupRemapRule[] remapRules, String intField, long itrTerm);
    public abstract void applyStringConditionsCallback(int n, ThreadSafeBitSet docRemapped, GroupRemapRule[] remapRules, String stringField, String itrTerm);
    public abstract int get(int doc);
    public abstract void set(int doc, int group);
    public abstract void batchSet(int[] docIdBuf, int[] docGrpBuffer, int n);
    public abstract void fill(int group);
    public abstract void copyInto(GroupLookup other);
    public abstract int size();
    public abstract int maxGroup();
    public abstract long memoryUsed();
    public abstract void fillDocGrpBuffer(int[] docIdBuf, int[] docGrpBuffer, int n);
    public abstract void fillDocGrpBufferSequential(int start, int[] docGrpBuffer, int n);
    public abstract void bitSetRegroup(FastBitSet bitSet, int targetGroup, int negativeGroup, int positiveGroup);
    public abstract ImhotepLocalSession getSession();
    protected abstract void recalculateNumGroups();

    public final int getNumGroups() {
        return numGroups;
    }
}
