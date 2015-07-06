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

import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

import java.util.Arrays;

class GroupStatCache {
    private static final int SIZEOF_LONG = 8;

    private final long[][]  stats;
    private final boolean[] dirty;
    private final MemoryReserver memory;

    public GroupStatCache(final int maxNumStats, final MemoryReserver memory) {
        this.stats  = new long[maxNumStats][];
        this.dirty  = new boolean[maxNumStats];
        this.memory = memory;
    }

    public long[] get(final int numStat) { return stats[numStat]; }

    public int maxNumStats() { return stats.length; } // !@# superfluous?

    public boolean    isDirty(final int numStat) { return dirty[numStat];  }
    public void    invalidate(final int numStat) { dirty[numStat] = true;  }
    public void      validate(final int numStat) { dirty[numStat] = false; }

    public void reset(final int numStats, final int numGroups)
        throws ImhotepOutOfMemoryException {
        for (int stat = 0; stat < numStats; ++stat) {
            resetStat(stat, numGroups);
        }
    }

    public void resetStat(final int stat, final int newLength)
        throws ImhotepOutOfMemoryException {

        invalidate(stat);

        if (stats[stat] != null && stats[stat].length > newLength) {
            Arrays.fill(stats[stat], 0);
            return;
        }

        if (stats[stat] != null) {
            memory.releaseMemory(stats[stat].length * SIZEOF_LONG);
        }

        if (!memory.claimMemory(newLength * SIZEOF_LONG)) {
            throw new ImhotepOutOfMemoryException();
        }

        stats[stat] = new long[newLength];
    }

    public void clearStat(final int stat) {
        final long freed = stats[stat].length * SIZEOF_LONG;
        memory.releaseMemory(freed);
        stats[stat] = null;
    }
}
