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

import com.google.common.collect.ComparisonChain;
import com.indeed.flamdex.query.Term;

import java.util.Comparator;

/**
 * @author jsgroth
 */
public final class TermCount {
    // sort by count then by term
    public static final Comparator<TermCount> COUNT_COMPARATOR = new Comparator<TermCount>() {
        @Override
        public int compare(TermCount o1, TermCount o2) {
            return ComparisonChain.start()
                    .compare(o1.count, o2.count)
                    .compare(o1.term.getTermIntVal(), o2.term.getTermIntVal())
                    .compare(o1.term.getTermStringVal(), o2.term.getTermStringVal())
                    .result();
        }
    };

    public static final Comparator<TermCount> REVERSE_COUNT_COMPARATOR = new Comparator<TermCount>() {
        @Override
        public int compare(TermCount o1, TermCount o2) {
            return -COUNT_COMPARATOR.compare(o1, o2);
        }
    };

    private final Term term;
    private final long count;

    public TermCount(Term term, long count) {
        this.term = term;
        this.count = count;
    }

    public Term getTerm() {
        return term;
    }

    public long getCount() {
        return count;
    }
}
