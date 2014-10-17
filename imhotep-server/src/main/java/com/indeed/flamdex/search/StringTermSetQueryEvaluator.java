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
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;

import java.util.Arrays;

/**
 * @author jwolfe
 */
public class StringTermSetQueryEvaluator implements QueryEvaluator {
    private static final int BUFFER_SIZE = 64;

    private final String field;
    private final String[] terms;

    public StringTermSetQueryEvaluator(final String field, final String[] terms) {
        this.field = field;
        this.terms = new String[terms.length];
        System.arraycopy(terms, 0, this.terms, 0, terms.length);
        Arrays.sort(this.terms);
    }

    @Override
    public void and(FlamdexReader r, FastBitSet bitSet, FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
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
    public void or(FlamdexReader r, FastBitSet bitSet, FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
        internalSearch(r, bitSet);
    }

    @Override
    public void not(FlamdexReader r, FastBitSet bitSet, FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
        bitSet.clearAll();
        internalSearch(r, bitSet);
        bitSet.invertAll();
    }

    private void internalSearch(FlamdexReader r, FastBitSet bitSet) {
        final StringTermIterator iterator = r.getStringTermIterator(field);
        int ix = 0;
        try {
            final DocIdStream docIdStream = r.getDocIdStream();
            try {
                while (ix < terms.length) {
                    final int[] docIdBuffer = new int[BUFFER_SIZE];
                    iterator.reset(terms[ix++]);
                    if (!iterator.next()) break;
                    if(!iterator.term().equals(terms[ix-1])) continue;
                    docIdStream.reset(iterator);
                    readDocIdStream(docIdStream, docIdBuffer, bitSet);
                }
            } finally {
                docIdStream.close();
            }
        } finally {
            iterator.close();
        }
    }

    private void readDocIdStream(DocIdStream docIdStream, int[] docIdBuffer, FastBitSet bitSet) {
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuffer);
            for (int i = 0; i < n; ++i) {
                bitSet.set(docIdBuffer[i]);
            }
            if (n < docIdBuffer.length) break;
        }
    }
}
