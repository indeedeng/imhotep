package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGAIterator;

/**
 * Base class for merging AggregateFTGSIterators that have disjoint terms
 *
 * Each term exists only in one iterator and
 * after nextTerm() call iterators[0] is iterator with current term.
 */
public abstract class AbstractDisjointFTGAMerger extends AbstractDisjointFTGMerger<FTGAIterator> implements FTGAIterator {
    private final int numStats;

    public AbstractDisjointFTGAMerger(final FTGAIterator[] iterators) {
        super(iterators);
        numStats = FTGSIteratorUtil.getNumStats(iterators);
    }

    @Override
    public int getNumStats() {
        return numStats;
    }

    @Override
    public final void groupStats(final double[] stats) {
        currentIterator().groupStats(stats);
    }
}