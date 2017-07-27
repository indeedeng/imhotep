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

    StringRangeQueryEvaluator(final Term startTerm, final Term endTerm, final boolean maxInclusive) {
        this.startTerm = startTerm;
        this.endTerm = endTerm;
        isMaxInclusive = maxInclusive;
        looksLikeAnIntQuery = isInt(startTerm.getTermStringVal()) && isInt(endTerm.getTermStringVal());
    }

    private static boolean isInt(final String s) {
        try {
            return Long.parseLong(s) >= 0;
        } catch (final NumberFormatException e) {
            return false;
        }
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

    private void internalSearch(final FlamdexReader r, final FastBitSet bitSet) {
        if (looksLikeAnIntQuery) {
            doIntRangeQuery(r, bitSet);
            return;
        }

        try (StringTermIterator iterator = r.getStringTermIterator(startTerm.getFieldName())) {
            try (DocIdStream docIdStream = r.getDocIdStream()) {
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

    private void doIntRangeQuery(final FlamdexReader r, final FastBitSet bitSet) {
        final long min = Long.parseLong(startTerm.getTermStringVal());
        final long max = Long.parseLong(endTerm.getTermStringVal());

        try (StringTermIterator iterator = r.getStringTermIterator(startTerm.getFieldName())) {
            try (DocIdStream docIdStream = r.getDocIdStream()) {
                final int[] docIdBuffer = new int[BUFFER_SIZE];
                while (iterator.next()) {
                    final long termIntVal;
                    try {
                        termIntVal = Long.parseLong(iterator.term());
                    } catch (final NumberFormatException e) {
                        continue;
                    }

                    if (termIntVal < min || (isMaxInclusive && termIntVal > max) || (!isMaxInclusive && termIntVal >= max)) {
                        continue;
                    }

                    docIdStream.reset(iterator);
                    readDocIdStream(docIdStream, docIdBuffer, bitSet);
                }
            }
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
}
