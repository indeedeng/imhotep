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
 package com.indeed.flamdex.simple;

import com.indeed.flamdex.api.IntTermDocIterator;

import java.io.IOException;

/**
 * @author jplaisance
 */
public final class NativeIntTermDocIterator extends NativeTermDocIterator<SimpleIntTermIterator> implements IntTermDocIterator {
    private final long[] bufferedTerms = new long[BUFFER_SIZE];

    public NativeIntTermDocIterator(final SimpleIntTermIterator termIterator,
                                    final MapCache mapCache,
                                    final boolean useSSSE3)
            throws IOException {
        super(mapCache, termIterator, useSSSE3);
    }

    @Override
    protected void cacheCurrentTerm(final int index) {
        bufferedTerms[index] = termIterator.term();
    }

    @Override
    protected void resetCache() {
    }

    @Override
    public long term() {
        return bufferedTerms[getBufferedTermIndex()];
    }

    @Override
    public int nextDocs(final int[] docIdBuffer) {
        return fillDocIdBuffer(docIdBuffer);
    }
}
