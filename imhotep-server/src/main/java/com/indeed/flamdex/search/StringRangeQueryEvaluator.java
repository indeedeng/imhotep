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
import com.indeed.flamdex.query.Term;

/**
 * @author jsgroth
 */
class StringRangeQueryEvaluator implements QueryEvaluator {
    private static final int BUFFER_SIZE = 64;

    private final Term startTerm;
    private final Term endTerm;
    private final boolean isMaxInclusive;
    private final boolean looksLikeAnIntQuery;

    StringRangeQueryEvaluator(Term startTerm, Term endTerm, boolean maxInclusive) {
        this.startTerm = startTerm;
        this.endTerm = endTerm;
        isMaxInclusive = maxInclusive;
        looksLikeAnIntQuery = isInt(startTerm.getTermStringVal()) && isInt(endTerm.getTermStringVal());
    }

    private static boolean isInt(String s) {
        try {
            return Long.parseLong(s) >= 0;
        } catch (NumberFormatException e) {
            return false;
        }
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

    private void internalSearch(FlamdexReader r, FastBitSet bitSet) {
        if (looksLikeAnIntQuery) {
            doIntRangeQuery(r, bitSet);
            return;
        }

        final StringTermIterator iterator = r.getStringTermIterator(startTerm.getFieldName());
        try {
            final DocIdStream docIdStream = r.getDocIdStream();
            try {
                final int[] docIdBuffer = new int[BUFFER_SIZE];
                iterator.reset(startTerm.getTermStringVal());
                if (isMaxInclusive) {
                    while (iterator.next() && iterator.term().compareTo(endTerm.getTermStringVal()) <= 0) {
                        docIdStream.reset(iterator);
                        readDocIdStream(docIdStream, docIdBuffer, bitSet);
                    }
                } else {
                    while (iterator.next() && iterator.term().compareTo(endTerm.getTermStringVal()) < 0) {
                        docIdStream.reset(iterator);
                        readDocIdStream(docIdStream, docIdBuffer, bitSet);
                    }
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

    private void doIntRangeQuery(FlamdexReader r, FastBitSet bitSet) {
        final long min = Long.parseLong(startTerm.getTermStringVal());
        final long max = Long.parseLong(endTerm.getTermStringVal());

        final StringTermIterator iterator = r.getStringTermIterator(startTerm.getFieldName());
        try {
            final DocIdStream docIdStream = r.getDocIdStream();
            try {
                final int[] docIdBuffer = new int[BUFFER_SIZE];
                while (iterator.next()) {
                    final long termIntVal;
                    try {
                        termIntVal = Long.parseLong(iterator.term());
                    } catch (NumberFormatException e) {
                        continue;
                    }

                    if (termIntVal < min || (isMaxInclusive && termIntVal > max) || (!isMaxInclusive && termIntVal >= max)) continue;

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
}
