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

import com.indeed.imhotep.api.GroupStatsIterator;
import it.unimi.dsi.fastutil.longs.AbstractLongIterator;

import java.util.NoSuchElementException;

/**
 * Simple wrapper for GroupStatsIterator over array of longs
 *
 * @author aibragimov
 */

public class GroupStatsDummyIterator extends AbstractLongIterator implements GroupStatsIterator {

    private long[] data;
    private int index;
    private final int effectiveSize; // size without trailing zeroes.

    public GroupStatsDummyIterator(final long[] data) {
        this.data = data;
        index = 0;
        int size = data.length;
        while ((size > 0) && (data[size - 1] == 0)) {
            size--;
        }
        effectiveSize = size;
    }

    @Override
    public int getNumGroups() {
        return effectiveSize;
    }

    @Override
    public boolean hasNext() {
        return index < effectiveSize;
    }

    @Override
    public long nextLong() {
        if(index >= effectiveSize) {
            throw new NoSuchElementException();
        }
        return data[index++];
    }

    @Override
    public void close() {
        data = null;
        index = effectiveSize; // just in case, to make hasNext() return false
    }
}