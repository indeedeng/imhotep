package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;

// FTGSIterator with not ordered terms.
public final class UnsortedFTGSIterator extends UnsortedFTGIterator<FTGSIterator> implements FTGSIterator {
    private final int numStats;

    public UnsortedFTGSIterator(final FTGSIterator[] iterators) {
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