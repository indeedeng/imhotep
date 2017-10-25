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
 package com.indeed.flamdex.search;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;

import java.util.Arrays;

/**
 * @author jwolfe
 */
public class IntTermSetQueryEvaluator implements QueryEvaluator {
    private static final int BUFFER_SIZE = 64;

    private final String field;
    private final long[] terms;

    public IntTermSetQueryEvaluator(final String field, final long[] terms) {
        this.field = field;
        this.terms = new long[terms.length];
        System.arraycopy(terms, 0, this.terms, 0, terms.length);
        Arrays.sort(this.terms);
    }

    @Override
    public void and(final FlamdexReader r, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
        FastBitSet localBitSet = bitSetPooler.create(r.getNumDocs());
        try {
            internalSearch(r, localBitSet);
            bitSet.and(localBitSet);
        } finally {
            final long memUsage = localBitSet.memoryUsage();
            localBitSet = null;
            bitSetPooler.release(memUsage);
        }
    }

    @Override
    public void or(final FlamdexReader r, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
        internalSearch(r, bitSet);
    }

    @Override
    public void not(final FlamdexReader r, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
        bitSet.clearAll();
        internalSearch(r, bitSet);
        bitSet.invertAll();
    }

    private void internalSearch(final FlamdexReader r, final FastBitSet bitSet) {
        try (IntTermIterator iterator = r.getIntTermIterator(field)) {
            try (DocIdStream docIdStream = r.getDocIdStream()) {
                final int[] docIdBuffer = new int[BUFFER_SIZE];
                for (final long term : terms) {
                    iterator.reset(term);
                    if (!iterator.next()) {
                        break;
                    }
                    if (iterator.term() != term) {
                        continue;
                    }
                    docIdStream.reset(iterator);
                    readDocIdStream(docIdStream, docIdBuffer, bitSet);
                }
            }
        }
    }

    private void readDocIdStream(final DocIdStream docIdStream, final int[] docIdBuffer, final FastBitSet bitSet) {
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuffer);
            for (int i = 0; i < n; ++i) {
                bitSet.set(docIdBuffer[i]);
            }
            if (n < docIdBuffer.length) {
                break;
            }
        }
    }
}
