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
 package com.indeed.flamdex.api;

import com.indeed.imhotep.MemoryMeasured;

public interface IntValueLookup extends MemoryMeasured {
    /**
     * @return a number less than or equal to the smallest int value in this lookup
     */
    long getMin();

    /**
     * @return a number greater than or equal to the largest int value in this lookup
     */
    long getMax();

    /**
     * @param docIds  The docIds for which to lookup values
     * @param values  The buffer in which to store retrieved values
     * @param n Only lookup values for the first n docIds
     */
    void lookup(int[] docIds, long[] values, int n);
}
