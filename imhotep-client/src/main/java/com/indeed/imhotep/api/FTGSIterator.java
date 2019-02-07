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
 package com.indeed.imhotep.api;

import java.io.Closeable;

public interface FTGSIterator extends FTGIterator, Closeable {
    /**
     * @return number of stats
     */
    int getNumStats();

    /**
     * @param stats array in which to store the stats associated with the current group
     */
    default void groupStats(long[] stats) {
        groupStats(stats, 0);
    }

    /**
     * @param stats array in which to store the stats associated with the current group
     * @param offset at which to place the stats
     * @throws ArrayIndexOutOfBoundsException if stats.length < offset + getNumStats()
     */
    void groupStats(long[] stats, int offset);
}
