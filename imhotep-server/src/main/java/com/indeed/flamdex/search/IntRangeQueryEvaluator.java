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

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;
import com.indeed.flamdex.query.Term;

/**
 * @author jsgroth
 */
class IntRangeQueryEvaluator implements QueryEvaluator {
    private static final int BUFFER_SIZE = 64;

    private final Term startTerm;
    private final Term endTerm;
    private final boolean isMaxInclusive;

    IntRangeQueryEvaluator(final Term startTerm, final Term endTerm, final boolean maxInclusive) {
        this.startTerm = startTerm;
        this.endTerm = endTerm;
        isMaxInclusive = maxInclusive;
    }

    @Override
    public void and(final FlamdexReader r, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
        try (IntValueLookup metric = r.getMetric(startTerm.getFieldName())) {
            internalAnd(metric, r.getNumDocs(), bitSet);
        }
    }

    private void internalAnd(final IntValueLookup metric, final int numDocs, final FastBitSet bitSet) {
        final long startVal = startTerm.getTermIntVal();
        final long endVal = endTerm.getTermIntVal();
        if (isMaxInclusive) {
            if (endVal < metric.getMin() || startVal > metric.getMax()) {
                bitSet.clearAll();
                return;
            }
            if (startVal <= metric.getMin() && endVal >= metric.getMax()) {
                return;
            }
            final int[] docBuf = new int[BUFFER_SIZE];
            final long[] valueBuf = new long[BUFFER_SIZE];
            int lastDoc = 0;
            for (int docStart = 0; docStart < numDocs; docStart += BUFFER_SIZE) {
                final int n = Math.min(numDocs, docStart + BUFFER_SIZE) - docStart;
                for (int i = 0; i < n; ++i) {
                    docBuf[i] = docStart + i;
                }
                metric.lookup(docBuf, valueBuf, n);
                for (int i = 0; i < n; ++i) {
                    final int doc = docBuf[i];
                    final long val = valueBuf[i];
                    if (val >= startVal && val <= endVal) {
                        if (lastDoc != doc) {
                            bitSet.clearRange(lastDoc, doc);
                        }
                        lastDoc = doc + 1;
                    }
                }
            }
            bitSet.clearRange(lastDoc, numDocs);
        } else {
            if (endVal <= metric.getMin() || startVal > metric.getMax()) {
                bitSet.clearAll();
                return;
            }
            if (startVal <= metric.getMin() && endVal > metric.getMax()) {
                return;
            }
            final int[] docBuf = new int[BUFFER_SIZE];
            final long[] valueBuf = new long[BUFFER_SIZE];
            int lastDoc = 0;
            for (int docStart = 0; docStart < numDocs; docStart += BUFFER_SIZE) {
                final int n = Math.min(numDocs, docStart + BUFFER_SIZE) - docStart;
                for (int i = 0; i < n; ++i) {
                    docBuf[i] = docStart + i;
                }
                metric.lookup(docBuf, valueBuf, n);
                for (int i = 0; i < n; ++i) {
                    final int doc = docBuf[i];
                    final long val = valueBuf[i];
                    if (val >= startVal && val < endVal) {
                        if (lastDoc != doc) {
                            bitSet.clearRange(lastDoc, doc);
                        }
                        lastDoc = doc + 1;
                    }
                }
            }
            bitSet.clearRange(lastDoc, numDocs);
        }
    }

    @Override
    public void or(final FlamdexReader r, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
        try (IntValueLookup metric = r.getMetric(startTerm.getFieldName())) {
            internalOr(metric, r.getNumDocs(), bitSet);
        }
    }

    private void internalOr(final IntValueLookup metric, final int numDocs, final FastBitSet bitSet) {
        final long startVal = startTerm.getTermIntVal();
        final long endVal = endTerm.getTermIntVal();
        if (isMaxInclusive) {
            if (endVal < metric.getMin() || startVal > metric.getMax()) {
                return;
            }
            if (startVal <= metric.getMin() && endVal >= metric.getMax()) {
                bitSet.setAll();
                return;
            }
            final int[] docBuf = new int[BUFFER_SIZE];
            final long[] valueBuf = new long[BUFFER_SIZE];
            for (int docStart = 0; docStart < numDocs; docStart += BUFFER_SIZE) {
                final int n = Math.min(numDocs, docStart + BUFFER_SIZE) - docStart;
                for (int i = 0; i < n; ++i) {
                    docBuf[i] = docStart + i;
                }
                metric.lookup(docBuf, valueBuf, n);
                for (int i = 0; i < n; ++i) {
                    final int doc = docBuf[i];
                    final long val = valueBuf[i];
                    if (val >= startVal && val <= endVal) {
                        bitSet.set(doc);
                    }
                }
            }
        } else {
            if (endVal <= metric.getMin() || startVal > metric.getMax()) {
                return;
            }
            if (startVal <= metric.getMin() && endVal > metric.getMax()) {
                bitSet.setAll();
                return;
            }
            final int[] docBuf = new int[BUFFER_SIZE];
            final long[] valueBuf = new long[BUFFER_SIZE];
            for (int docStart = 0; docStart < numDocs; docStart += BUFFER_SIZE) {
                final int n = Math.min(numDocs, docStart + BUFFER_SIZE) - docStart;
                for (int i = 0; i < n; ++i) {
                    docBuf[i] = docStart + i;
                }
                metric.lookup(docBuf, valueBuf, n);
                for (int i = 0; i < n; ++i) {
                    final int doc = docBuf[i];
                    final long val = valueBuf[i];
                    if (val >= startVal && val < endVal) {
                        bitSet.set(doc);
                    }
                }
            }
        }
    }

    @Override
    public void not(final FlamdexReader r, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException {
        try (IntValueLookup metric = r.getMetric(startTerm.getFieldName())) {
            internalNot(metric, r.getNumDocs(), bitSet);
        }
    }

    private void internalNot(final IntValueLookup metric, final int numDocs, final FastBitSet bitSet) {
        final long startVal = startTerm.getTermIntVal();
        final long endVal = endTerm.getTermIntVal();
        if (isMaxInclusive) {
            if (endVal < metric.getMin() || startVal > metric.getMax()) {
                bitSet.setAll();
                return;
            }
            final int[] docBuf = new int[BUFFER_SIZE];
            final long[] valueBuf = new long[BUFFER_SIZE];
            int lastDoc = 0;
            for (int docStart = 0; docStart < numDocs; docStart += BUFFER_SIZE) {
                final int n = Math.min(numDocs, docStart + BUFFER_SIZE) - docStart;
                for (int i = 0; i < n; ++i) {
                    docBuf[i] = docStart + i;
                }
                metric.lookup(docBuf, valueBuf, n);
                for (int i = 0; i < n; ++i) {
                    final int doc = docBuf[i];
                    final long val = valueBuf[i];
                    if (val >= startVal && val <= endVal) {
                        if (lastDoc != doc) {
                            bitSet.setRange(lastDoc, doc);
                        }
                        bitSet.clear(doc);
                        lastDoc = doc + 1;
                    }
                }
            }
            bitSet.setRange(lastDoc, numDocs);
        } else {
            if (endVal <= metric.getMin() || startVal > metric.getMax()) {
                bitSet.setAll();
                return;
            }
            final int[] docBuf = new int[BUFFER_SIZE];
            final long[] valueBuf = new long[BUFFER_SIZE];
            int lastDoc = 0;
            for (int docStart = 0; docStart < numDocs; docStart += BUFFER_SIZE) {
                final int n = Math.min(numDocs, docStart + BUFFER_SIZE) - docStart;
                for (int i = 0; i < n; ++i) {
                    docBuf[i] = docStart + i;
                }
                metric.lookup(docBuf, valueBuf, n);
                for (int i = 0; i < n; ++i) {
                    final int doc = docBuf[i];
                    final long val = valueBuf[i];
                    if (val >= startVal && val < endVal) {
                        if (lastDoc != doc) {
                            bitSet.setRange(lastDoc, doc);
                        }
                        bitSet.clear(doc);
                        lastDoc = doc + 1;
                    }
                }
            }
            bitSet.setRange(lastDoc, numDocs);
        }
    }
}
