package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;

// FGSTIterator with not ordered terms.
public final class UnsortedFTGSIterator extends AbstractDisjointFTGSMerger {
    public UnsortedFTGSIterator(final FTGSIterator[] iterators) {
        super(iterators);
    }

    @Override
    public boolean nextTerm() {
        while (true) {
            if (numFieldIterators == 0) {
                return false;
            }
            if (iterators[0].nextTerm()) {
                return true;
            }
            numFieldIterators--;
            final FTGSIterator tmp = iterators[0];
            System.arraycopy(iterators, 1, iterators, 0, numFieldIterators);
            iterators[numFieldIterators] = tmp;
        }
    }
}