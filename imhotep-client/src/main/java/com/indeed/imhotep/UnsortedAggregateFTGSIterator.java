package com.indeed.imhotep;

import com.indeed.imhotep.api.AggregateFTGSIterator;

// FTGSIterator with not ordered terms.
public final class UnsortedAggregateFTGSIterator extends UnsortedFTGIterator<AggregateFTGSIterator> implements AggregateFTGSIterator {
    private final int numStats;

    public UnsortedAggregateFTGSIterator(final AggregateFTGSIterator[] iterators) {
        super(iterators);
        numStats = FTGSIteratorUtil.getNumStats(iterators);
    }

    @Override
    public int getNumStats() {
        return numStats;
    }

    @Override
    public void groupStats(double[] stats) {
        currentIterator().groupStats(stats);
    }
}