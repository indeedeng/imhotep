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

import java.util.List;

import com.indeed.util.core.Pair;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.TermDocIterator;

public class MergingStringTermDocIterator extends MergingTermDocIterator implements
        StringTermDocIterator {
    private final String[] nextTerms;
    private String currentTerm;

    @SuppressWarnings("unchecked")
    public MergingStringTermDocIterator(List<StringTermDocIterator> tdIters,
                                        int[] mapping,
                                        List<Integer> iterNumToDocOffset) {
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
            if (currentTerm != null && ! currentTerm.equals(nextTerms[i]))
                continue;
            StringTermDocIterator iter = (StringTermDocIterator) iters.get(i);
            if (iter.nextTerm())
                nextTerms[i] = iter.term();
            else
                nextTerms[i] = null;
        }

        /* second find the smallest term */
        String min = null;
        for (int i = 0; i < nextTerms.length; i++) {
            if (nextTerms[i] == null)
                continue;
            if (min == null || nextTerms[i].compareTo(min) < 0)
                min = nextTerms[i];
        }
        currentTerm = min;

        /* check if all the iterators are done */
        if (currentTerm == null)
            return false;

        /* track which iterators have this terms */
        itersAndOffsetsForTerm.clear();
        for (int i = 0; i < nextTerms.length; i++) {
            if (nextTerms[i] != null && currentTerm.equals(nextTerms[i])) {
                int offset = iterNumToDocOffset.get(i);
                itersAndOffsetsForTerm
                        .add(new Pair<Integer, TermDocIterator>(offset, iters.get(i)));
            }
        }

        return true;
    }

    @Override
    public String term() {
        return currentTerm;
    }

}
