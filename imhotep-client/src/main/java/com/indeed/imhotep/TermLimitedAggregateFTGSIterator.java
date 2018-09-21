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

import com.indeed.imhotep.api.AggregateFTGSIterator;


/**
 * Wrapper for an FTGSIterator that will only return up to 'termLimit' terms that have at least 1 group.
 * Terms that don't have at least 1 non-0 group are not counted.
 * @author vladimir
 */

public class TermLimitedAggregateFTGSIterator extends TermLimitedFTGIterator<AggregateFTGSIterator> implements AggregateFTGSIterator {
    /**
     * @param wrapped The iterator to use
     * @param termLimit Maximum number of terms that will be allowed to iterate through
     */
    public TermLimitedAggregateFTGSIterator(final AggregateFTGSIterator wrapped, final long termLimit) {
        super(wrapped, termLimit);
    }

    @Override
    public int getNumStats() {
        return wrapped.getNumStats();
    }

    @Override
    public void groupStats(final double[] stats) {
        wrapped.groupStats(stats);
    }
}
