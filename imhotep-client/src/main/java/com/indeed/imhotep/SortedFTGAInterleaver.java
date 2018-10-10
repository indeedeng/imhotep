package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGAIterator;

public class SortedFTGAInterleaver extends SortedFTGInterleaver<FTGAIterator> implements FTGAIterator {
    private final int numStats;

    public SortedFTGAInterleaver(FTGAIterator[] iterators) {
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