package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;

public class SortedFTGSInterleaver extends SortedFTGInterleaver<FTGSIterator> implements FTGSIterator {
    private final int numStats;

    public SortedFTGSInterleaver(FTGSIterator[] iterators) {
        super(iterators);
        numStats = FTGSIteratorUtil.getNumStats(iterators);
    }

    @Override
    public int getNumStats() {
        return numStats;
    }

    @Override
    public void groupStats(long[] stats) {
        currentIterator().groupStats(stats);
    }
}