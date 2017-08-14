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

import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.TermDocIterator;
import com.indeed.util.core.Pair;

import java.util.List;

public class MergingStringTermDocIterator extends MergingTermDocIterator implements
        StringTermDocIterator {
    private final String[] nextTerms;
    private String currentTerm;

    @SuppressWarnings("unchecked")
    public MergingStringTermDocIterator(final List<StringTermDocIterator> tdIters,
                                        final int[] mapping,
                                        final List<Integer> iterNumToDocOffset) {
        super((List<TermDocIterator>) (List<?>) tdIters, mapping, iterNumToDocOffset);
        this.nextTerms = new String[iters.size()];
        this.currentTerm = null;
    }

    @Override
    public boolean nextTerm() {
        /*
         * find smallest term from all the iterators
         */
        /* first, update the list of next terms for all iterators */
        for (int i = 0; i < nextTerms.length; i++) {
            if (currentTerm != null && ! currentTerm.equals(nextTerms[i])) {
                continue;
            }
            final StringTermDocIterator iter = (StringTermDocIterator) iters.get(i);
            if (iter.nextTerm()) {
                nextTerms[i] = iter.term();
            } else {
                nextTerms[i] = null;
            }
        }

        /* second find the smallest term */
        String min = null;
        for (final String nextTerm : nextTerms) {
            if (nextTerm == null) {
                continue;
            }
            if (min == null || nextTerm.compareTo(min) < 0) {
                min = nextTerm;
            }
        }
        currentTerm = min;

        /* check if all the iterators are done */
        if (currentTerm == null) {
            return false;
        }

        /* track which iterators have this terms */
        itersAndOffsetsForTerm.clear();
        for (int i = 0; i < nextTerms.length; i++) {
            if (nextTerms[i] != null && currentTerm.equals(nextTerms[i])) {
                final int offset = iterNumToDocOffset.get(i);
                itersAndOffsetsForTerm
                        .add(new Pair<>(offset, iters.get(i)));
            }
        }

        return true;
    }

    @Override
    public String term() {
        return currentTerm;
    }

}
