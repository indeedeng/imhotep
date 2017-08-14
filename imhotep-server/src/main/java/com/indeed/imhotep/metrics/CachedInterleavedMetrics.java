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
 package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

import java.util.BitSet;

/**
 * @author dwahler
 */
public class CachedInterleavedMetrics {
    private final MemoryReserver memory;

    private final int numDocs, stride;
    private long[] interleavedData;

    private final BitSet closed;
    private boolean released;

    public CachedInterleavedMetrics(
            final MemoryReserver memory,
            final int numDocs,
            final IntValueLookup... lookups) throws ImhotepOutOfMemoryException {
        this.memory = memory;
        this.numDocs = numDocs;
        this.stride = lookups.length;

        if (!memory.claimMemory(numDocs * 8L * stride)) {
            throw new ImhotepOutOfMemoryException();
        }
        interleavedData = new long[numDocs * stride];
        fillValues(lookups);

        closed = new BitSet(stride);
    }

    public IntValueLookup[] getLookups() {
        final IntValueLookup[] result = new IntValueLookup[stride];
        for (int i = 0; i < stride; i++) {
            result[i] = new InterleavedLookup(i);
        }
        return result;
    }

    private void fillValues(final IntValueLookup... lookups) {
        final int BUFFER_SIZE = 8192;
        final int[] idBuffer = new int[BUFFER_SIZE];
        final long[] valBuffer = new long[BUFFER_SIZE];

        for (int start = 0; start < numDocs; start += BUFFER_SIZE) {
            final int end = Math.min(numDocs, start+BUFFER_SIZE);
            final int n = end-start;
            for (int i = 0; i < n; i++) {
                idBuffer[i] = start + i;
            }

            for (int offset = 0; offset < lookups.length; offset++) {
                lookups[offset].lookup(idBuffer, valBuffer, n);
                for (int i = 0; i < n; i++) {
                    interleavedData[(i+start)*stride+offset] = valBuffer[i];
                }
            }
        }
    }

    private void closeLookup(final int offset) {
        closed.set(offset);
        if (closed.nextClearBit(0) == stride && !released) {
            // all lookups are closed; release memory from pool
            released = true;
            interleavedData = null;
            memory.releaseMemory(numDocs * 8L * stride);
        }
    }

    private class InterleavedLookup implements IntValueLookup {
        private final int offset;

        private InterleavedLookup(final int offset) {
            this.offset = offset;
        }

        @Override
        public long getMin() {
            return Long.MIN_VALUE; // TODO compute this
        }

        @Override
        public long getMax() {
            return Long.MAX_VALUE; // TODO compute this;
        }

        @Override
        public void lookup(final int[] docIds, final long[] values, final int n) {
            final long[] interleavedData = CachedInterleavedMetrics.this.interleavedData;
            final int stride = CachedInterleavedMetrics.this.stride;
            final int offset = this.offset;

            for (int i = 0; i < n; i++) {
                values[i] = interleavedData[docIds[i]*stride + offset];
            }
        }

        @Override
        public long memoryUsed() {
            return numDocs * 8L;
        }

        @Override
        public void close() {
            closeLookup(offset);
        }
    }
}
