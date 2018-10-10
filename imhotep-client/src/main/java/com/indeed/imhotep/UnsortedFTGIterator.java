package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGIterator;

// FTGIterator with not ordered terms.
public class UnsortedFTGIterator<T extends FTGIterator> extends AbstractDisjointFTGMerger<T> {
    public UnsortedFTGIterator(final T[] iterators) {
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
            final T tmp = iterators[0];
            System.arraycopy(iterators, 1, iterators, 0, numFieldIterators);
            iterators[numFieldIterators] = tmp;
        }
    }
}