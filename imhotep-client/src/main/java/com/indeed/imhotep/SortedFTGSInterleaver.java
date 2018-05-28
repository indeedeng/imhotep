package com.indeed.imhotep;

import com.google.common.primitives.Longs;
import com.indeed.imhotep.api.FTGSIterator;

public final class SortedFTGSInterleaver extends AbstractDisjointFTGSMerger {

    private boolean initialized = false;

    public SortedFTGSInterleaver(final FTGSIterator[] iterators) {
        super(iterators);
    }

    @Override
    public boolean nextField() {
        initialized = false;
        return super.nextField();
    }

    @Override
    public boolean nextTerm() {
        if (!initialized) {
            initialized = true;
            for (int i = iterators.length-1; i >= 0; i--) {
                if (!iterators[i].nextTerm()) {
                    numFieldIterators--;
                    swap(i, numFieldIterators);
                }
            }
            for (int i = numFieldIterators-1; i >= 0; i--) {
                downHeap(i);
            }
            return numFieldIterators > 0;
        }
        if (numFieldIterators <= 0) {
            return false;
        }
        if (!iterators[0].nextTerm()) {
            numFieldIterators--;
            if (numFieldIterators <= 0) {
                return false;
            }
            swap(0, numFieldIterators);
        }
        downHeap(0);
        return true;
    }

    private void downHeap(int index) {
        while (true) {
            final int leftIndex = index * 2 + 1;
            final int rightIndex = leftIndex+1;
            if (leftIndex < numFieldIterators) {
                if (fieldIsIntType) {
                    final int lowerIndex = (rightIndex >= numFieldIterators) ?
                            leftIndex :
                            ((compareIntTerms(leftIndex, rightIndex) <= 0) ?
                                    leftIndex :
                                    rightIndex);
                    if (compareIntTerms(lowerIndex, index) < 0) {
                        swap(index, lowerIndex);
                        index = lowerIndex;
                    } else {
                        break;
                    }
                } else {
                    final int lowerIndex;
                    if (rightIndex >= numFieldIterators) {
                        lowerIndex = leftIndex;
                    } else {
                        lowerIndex = (compareStringTerms(leftIndex, rightIndex) <= 0) ?
                                leftIndex :
                                rightIndex;
                    }
                    if (compareStringTerms(lowerIndex, index) < 0) {
                        swap(index, lowerIndex);
                        index = lowerIndex;
                    } else {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    private int compareIntTerms(final int a, final int b) {
        return Longs.compare(iterators[a].termIntVal(), iterators[b].termIntVal());
    }

    private int compareStringTerms(final int a, final int b) {
        final FTGSIterator itA = iterators[a];
        final FTGSIterator itB = iterators[b];
        return FTGSMerger.compareBytes(itA.termStringBytes(), itA.termStringLength(), itB.termStringBytes(), itB.termStringLength());
    }

    private void swap(final int a, final int b) {
        final FTGSIterator tmp = iterators[a];
        iterators[a] = iterators[b];
        iterators[b] = tmp;
    }
}