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
            swap(0, numFieldIterators);
        }
    }

    private void swap(final int a, final int b) {
        final FTGSIterator tmp = iterators[a];
        iterators[a] = iterators[b];
        iterators[b] = tmp;
    }
}