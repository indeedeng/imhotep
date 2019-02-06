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

package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;

/**
 * @author kenh
 */

public class TopTermsFTGSIterator extends TopTermsFTGIterator<long[]> implements FTGSIterator {
    private final int numStats;

    public TopTermsFTGSIterator(final FTGSIteratorUtil.TopTermsStatsByField<long[]> topTermFTGS,
                                final int numStats,
                                final int numGroups) {
        super(topTermFTGS, numGroups);
        this.numStats = numStats;
    }

    @Override
    public int getNumStats() {
        return numStats;
    }

    @Override
    public void groupStats(final long[] stats, final int offset) {
        if (currentGroup == null) {
            throw new IllegalStateException("Invoked while not positioned in group");
        }
        System.arraycopy(currentGroup.groupStats, 0, stats, offset, currentGroup.groupStats.length);
    }
}
