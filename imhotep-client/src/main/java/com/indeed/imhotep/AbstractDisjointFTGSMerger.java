package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;

/**
 * Base class for merging FTGSIterators that have disjoint terms
 *
 * Each term exists only in one iterator and
 * after nextTerm() call iterators[0] is iterator with current term.
 */
public abstract class AbstractDisjointFTGSMerger extends AbstractDisjointFTGMerger<FTGSIterator> implements FTGSIterator {
    private final int numStats;

    public AbstractDisjointFTGSMerger(final FTGSIterator[] iterators) {
        super(iterators);
        numStats = FTGSIteratorUtil.getNumStats(iterators);
    }

    @Override
    public int getNumStats() {
        return numStats;
    }

    @Override
    public final void groupStats(final long[] stats, final int offset) {
        currentIterator().groupStats(stats, offset);
    }
}