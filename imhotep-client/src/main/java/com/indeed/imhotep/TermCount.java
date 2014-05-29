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
