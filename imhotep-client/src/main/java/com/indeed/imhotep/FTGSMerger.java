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
 package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Collection;

/**
 * @author jsgroth
 */
public final class FTGSMerger extends AbstractFTGSMerger {
    private String termStringVal;

    public FTGSMerger(Collection<? extends FTGSIterator> iterators, int numStats, @Nullable Closeable doneCallback) {
        super(iterators, numStats, doneCallback);
    }

    @Override
    public boolean nextTerm() {
        for (int i = 0; i < numTermIterators; ++i) {
            final FTGSIterator itr = iterators[termIterators[i]];
            if (!itr.nextTerm()) {
                final int fi = termIteratorIndexes[i];
                swap(fieldIterators, fi, --numFieldIterators);
                for (int j = 0; j < numTermIterators; ++j) {
                    if (termIteratorIndexes[j] == numFieldIterators) {
                        termIteratorIndexes[j] = fi;
                    }
                }
            }
        }

        numTermIterators = 0;
        if (numFieldIterators == 0) return false;

        int newNumTermIterators = 0;
        if (fieldIsIntType) {
            long min = Long.MAX_VALUE;
            for (int i = 0; i < numFieldIterators; ++i) {
                final FTGSIterator itr = iterators[fieldIterators[i]];
                final long term = itr.termIntVal();
                if (term < min) {
                    newNumTermIterators = 1;
                    termIteratorIndexes[0] = i;
                    min = term;
                } else if (term == min) {
                    termIteratorIndexes[newNumTermIterators++] = i;
                }
            }
            termIntVal = min;
        } else {
            String min = null;
            for (int i = 0; i < numFieldIterators; ++i) {
                final FTGSIterator itr = iterators[fieldIterators[i]];
                final String term = itr.termStringVal();
                final int c;
                if (min == null || (c = term.compareTo(min)) < 0) {
                    newNumTermIterators = 1;
                    termIteratorIndexes[0] = i;
                    min = term;
                } else if (c == 0) {
                    termIteratorIndexes[newNumTermIterators++] = i;
                }
            }
            termStringVal = min;
        }

        for (int i = 0; i < newNumTermIterators; ++i) {
            final int fi = termIteratorIndexes[i];
            final int index = fieldIterators[fi];
            termIterators[numTermIterators] = index;
            termIteratorIndexes[numTermIterators++] = fi;
        }
        termIteratorsRemaining = numTermIterators;
        for (int i = 0; i < termIteratorsRemaining; ++i) {
            final FTGSIterator itr = iterators[termIterators[i]];
            if (!itr.nextGroup()) {
                swap(termIterators, i, --termIteratorsRemaining);
                swap(termIteratorIndexes, i, termIteratorsRemaining);
                --i;
            }
        }
        accumulatedVec.reset();
        return true;
    }

    @Override
    public String termStringVal() {
        return termStringVal;
    }
}
