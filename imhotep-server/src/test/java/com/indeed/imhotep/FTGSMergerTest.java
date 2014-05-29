package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.RawFTGSIterator;

import java.util.Collection;

public class FTGSMergerTest extends AbstractFTGSMergerCase {
    @Override
    protected FTGSIterator newFTGSMerger(Collection<? extends RawFTGSIterator> iterators, int numStats) {
        return new FTGSMerger(iterators, numStats, null);
    }
}