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
 package com.indeed.flamdex.simple;

import com.google.common.base.Charsets;
import com.indeed.flamdex.api.RawStringTermDocIterator;

import java.io.IOException;

/**
 * @author jplaisance
 */
public final class NativeStringTermDocIterator extends NativeTermDocIterator<SimpleStringTermIterator> implements RawStringTermDocIterator {
    // terms are stored in one byte array
    private byte[] bufferedTerms = new byte[4096];
    private int bufferLength = 0;
    // start and len of cached terms.
    private final int[] lengths = new int[BUFFER_SIZE];
    private final int[] termStart = new int[BUFFER_SIZE];

    // cache for current term
    private int cachedTermIndex;
    private byte[] cachedTermBuffer = new byte[128];
    private String cachedTerm;

    public NativeStringTermDocIterator(final SimpleStringTermIterator termIterator,
                                       final MapCache mapCache,
                                       final boolean useSSSE3)
            throws IOException {
        super(mapCache, termIterator, useSSSE3);
    }

    @Override
    protected void cacheCurrentTerm(final int index) {
        final int termLength = termIterator.termStringLength();
        final byte[] termBytes = termIterator.termStringBytes();
        if ((bufferLength + termLength) > bufferedTerms.length) {
            final byte[] newBuffer = new byte[Math.max(bufferedTerms.length*2, bufferLength + termLength)];
            System.arraycopy(bufferedTerms, 0, newBuffer, 0, bufferLength);
            bufferedTerms = newBuffer;
        }

        termStart[index] = bufferLength;
        lengths[index] = termLength;
        System.arraycopy(termBytes, 0, bufferedTerms, bufferLength, termLength);
        bufferLength+=termLength;
    }

    @Override
    protected void resetCache() {
        bufferLength = 0;
        cachedTermIndex = -1;
    }

    @Override
    public String term() {
        if ((cachedTermIndex != getBufferedTermIndex()) || (cachedTerm == null)) {
            cachedTerm = new String(termStringBytes(), 0, termStringLength(), Charsets.UTF_8);
        }
        return cachedTerm;
    }

    @Override
    public byte[] termStringBytes() {
        final int termIndex = getBufferedTermIndex();
        if (cachedTermIndex != termIndex) {
            if (lengths[termIndex] > cachedTermBuffer.length ) {
                cachedTermBuffer = new byte[Math.max(cachedTermBuffer.length*2, lengths[termIndex])];
            }
            System.arraycopy(bufferedTerms, termStart[termIndex], cachedTermBuffer, 0, lengths[termIndex]);
            cachedTermIndex = termIndex;
            cachedTerm = null;
        }
        return cachedTermBuffer;
    }

    @Override
    public int termStringLength() {
        return lengths[getBufferedTermIndex()];
    }
}
