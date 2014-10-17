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
 package com.indeed.imhotep.local;

import java.util.Arrays;
import java.util.List;

import com.indeed.util.core.Pair;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.TermDocIterator;

public class MergingIntTermDocIterator extends MergingTermDocIterator implements IntTermDocIterator {
    private final long[] nextTerms;
    private long currentTerm;

    @SuppressWarnings("unchecked")
    public MergingIntTermDocIterator(List<IntTermDocIterator> tdIters,
                                     int[] mapping,
                                     List<Integer> iterNumToDocOffset) {
        super((List<TermDocIterator>) (List<?>) tdIters, mapping, iterNumToDocOffset);
        this.nextTerms = new long[iters.size()];
        currentTerm = Integer.MAX_VALUE;
        Arrays.fill(this.nextTerms, currentTerm);
    }

    @Override
    public boolean nextTerm() {
        /*
         * find smallest term from all the iterators
         */
        /* first, update the list of next terms for all iterators */
        for (int i = 0; i < nextTerms.length; i++) {
            if (currentTerm != nextTerms[i])
                continue;
            IntTermDocIterator iter = (IntTermDocIterator) iters.get(i);
            if (iter.nextTerm())
                nextTerms[i] = iter.term();
            else
                nextTerms[i] = Integer.MAX_VALUE;
        }

        /* second find the smallest term */
        long min = Integer.MAX_VALUE;
        for (int i = 0; i < nextTerms.length; i++) {
            if (nextTerms[i] == Integer.MAX_VALUE)
                continue;
            if (min > nextTerms[i])
                min = nextTerms[i];
        }
        currentTerm = min;

        /* check if all the iterators are done */
        if (currentTerm == Integer.MAX_VALUE)
            return false;

        /* track which iterators have this terms */
        itersAndOffsetsForTerm.clear();
        for (int i = 0; i < nextTerms.length; i++) {
            if (nextTerms[i] == currentTerm) {
                int offset = iterNumToDocOffset.get(i);
                itersAndOffsetsForTerm
                        .add(new Pair<Integer, TermDocIterator>(offset, iters.get(i)));
            }
        }

        return true;
    }

    @Override
    public int nextDocs(int[] docIdBuffer) {
        return fillDocIdBuffer(docIdBuffer);
    }

    @Override
    public long term() {
        return currentTerm;
    }

}
