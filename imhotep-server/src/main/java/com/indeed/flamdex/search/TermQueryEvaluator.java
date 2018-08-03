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
 package com.indeed.flamdex.search;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;
import com.indeed.flamdex.query.Term;

/**
 * @author jsgroth
 */
class TermQueryEvaluator implements QueryEvaluator {
    private final Term term;

    TermQueryEvaluator(final Term term) {
        this.term = term;
    }

    @Override
    public void and(final FlamdexReader r, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) {
        try(final DocIdStream docIdStream = r.getDocIdStream()) {
            if (term.isIntField()) {
                try (final IntTermIterator iterator = r.getUnsortedIntTermIterator(term.getFieldName())) {
                    intAnd(iterator, docIdStream, bitSet);
                }
            } else {
                try (final StringTermIterator iterator = r.getStringTermIterator(term.getFieldName())) {
                    stringAnd(iterator, docIdStream, bitSet);
                }
            }
        }
    }

    private void intAnd(final IntTermIterator iterator, final DocIdStream docIdStream, final FastBitSet bitSet) {
        final long termIntVal = term.getTermIntVal();
        iterator.reset(termIntVal);
        if (!iterator.next() || iterator.term() != termIntVal) {
            bitSet.clearAll();
            return;
        }
        internalAnd(iterator, docIdStream, bitSet);
    }

    private void stringAnd(final StringTermIterator iterator, final DocIdStream docIdStream, final FastBitSet bitSet) {
        final String termStringVal = term.getTermStringVal();
        iterator.reset(termStringVal);
        if (!iterator.next() || !iterator.term().equals(termStringVal)) {
            bitSet.clearAll();
            return;
        }
        internalAnd(iterator, docIdStream, bitSet);
    }

    private void internalAnd(final TermIterator iterator, final DocIdStream docIdStream, final FastBitSet bitSet) {
        docIdStream.reset(iterator);
        final int[] docIdBuffer = new int[64];
        int lastDoc = 0;
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuffer);
            for (int i = 0; i < n; ++i) {
                final int doc = docIdBuffer[i];
                bitSet.clearRange(lastDoc, doc);
                lastDoc = doc + 1;
            }
            if (n < docIdBuffer.length) {
                break;
            }
        }
        bitSet.clearRange(lastDoc, bitSet.size());
    }

    @Override
    public void or(final FlamdexReader r, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) {
        try (final DocIdStream docIdStream = r.getDocIdStream()) {
            if (term.isIntField()) {
                try (final IntTermIterator iterator = r.getUnsortedIntTermIterator(term.getFieldName())) {
                    intOr(iterator, docIdStream, bitSet);
                }
            } else {
                try (final StringTermIterator iterator = r.getStringTermIterator(term.getFieldName())) {
                    stringOr(iterator, docIdStream, bitSet);
                }
            }
        }
    }

    private void intOr(final IntTermIterator iterator, final DocIdStream docIdStream, final FastBitSet bitSet) {
        final long termIntVal = term.getTermIntVal();
        iterator.reset(termIntVal);
        if (!iterator.next() || iterator.term() != termIntVal) {
            return;
        }
        internalOr(iterator, docIdStream, bitSet);
    }

    private void stringOr(final StringTermIterator iterator, final DocIdStream docIdStream, final FastBitSet bitSet) {
        final String termStringVal = term.getTermStringVal();
        iterator.reset(termStringVal);
        if (!iterator.next() || !iterator.term().equals(termStringVal)) {
            return;
        }
        internalOr(iterator, docIdStream, bitSet);
    }

    private void internalOr(final TermIterator iterator, final DocIdStream docIdStream, final FastBitSet bitSet) {
        docIdStream.reset(iterator);
        final int[] docIdBuffer = new int[64];
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

    @Override
    public void not(final FlamdexReader r, final FastBitSet bitSet, final FastBitSetPooler bitSetPooler) {
        try (final DocIdStream docIdStream = r.getDocIdStream()) {
            if (term.isIntField()) {
                try (final IntTermIterator iterator = r.getUnsortedIntTermIterator(term.getFieldName())) {
                    intNot(iterator, docIdStream, bitSet);
                }
            } else {
                try (final StringTermIterator iterator = r.getStringTermIterator(term.getFieldName())) {
                    stringNot(iterator, docIdStream, bitSet);
                }
            }
        }
    }

    private void intNot(final IntTermIterator iterator, final DocIdStream docIdStream, final FastBitSet bitSet) {
        final long termIntVal = term.getTermIntVal();
        iterator.reset(termIntVal);
        if (!iterator.next() || iterator.term() != termIntVal) {
            bitSet.setAll();
            return;
        }
        internalNot(iterator, docIdStream, bitSet);
    }

    private void stringNot(final StringTermIterator iterator, final DocIdStream docIdStream, final FastBitSet bitSet) {
        final String termStringVal = term.getTermStringVal();
        iterator.reset(termStringVal);
        if (!iterator.next() || !iterator.term().equals(termStringVal)) {
            bitSet.setAll();
            return;
        }
        internalNot(iterator, docIdStream, bitSet);
    }

    private void internalNot(final TermIterator iterator, final DocIdStream docIdStream, final FastBitSet bitSet) {
        docIdStream.reset(iterator);
        final int[] docIdBuffer = new int[64];
        int lastDoc = 0;
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuffer);
            for (int i = 0; i < n; ++i) {
                final int doc = docIdBuffer[i];
                bitSet.setRange(lastDoc, doc);
                bitSet.clear(doc);
                lastDoc = doc + 1;
            }
            if (n < docIdBuffer.length) {
                break;
            }
        }
        bitSet.setRange(lastDoc, bitSet.size());
    }
}
