package com.indeed.imhotep;

import com.indeed.imhotep.api.AggregateFTGSIterator;

public class SortedAggregateFTGSInterleaver extends SortedFTGInterleaver<AggregateFTGSIterator> implements AggregateFTGSIterator {
    private final int numStats;

    public SortedAggregateFTGSInterleaver(AggregateFTGSIterator[] iterators) {
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