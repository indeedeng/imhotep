package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGAIterator;

// FTGSIterator with not ordered terms.
public final class UnsortedFTGAIterator extends UnsortedFTGIterator<FTGAIterator> implements FTGAIterator {
    private final int numStats;

    public UnsortedFTGAIterator(final FTGAIterator[] iterators) {
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