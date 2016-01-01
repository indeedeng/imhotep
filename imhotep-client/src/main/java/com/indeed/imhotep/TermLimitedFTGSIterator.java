package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;


/**
 * Wrapper for an FTGSIterator that will only return up to 'termLimit' terms.
 * @author vladimir
 */

public class TermLimitedFTGSIterator implements FTGSIterator {
    private final FTGSIterator wrapped;
    private final long termLimit;
    private long termsIterated = 0;

    /**
     * @param wrapped The iterator to use
     * @param termLimit Maximum number of terms that will be allowed to iterate through
     */
    public TermLimitedFTGSIterator(FTGSIterator wrapped, long termLimit) {
        this.wrapped = wrapped;
        this.termLimit = termLimit > 0 ? termLimit : Long.MAX_VALUE;
    }

    @Override
    public boolean nextField() {
        return wrapped.nextField();
    }

    @Override
    public String fieldName() {
        return wrapped.fieldName();
    }

    @Override
    public boolean fieldIsIntType() {
        return wrapped.fieldIsIntType();
    }

    @Override
    public boolean nextTerm() {
        if (termsIterated >= termLimit) {
            return false;
        }
        boolean hasNext = wrapped.nextTerm();
        if (hasNext) {
            termsIterated++;
        }
        return hasNext;
    }

    @Override
    public long termDocFreq() {
        return wrapped.termDocFreq();
    }

    @Override
    public long termIntVal() {
        return wrapped.termIntVal();
    }

    @Override
    public String termStringVal() {
        return wrapped.termStringVal();
    }

    @Override
    public boolean nextGroup() {
        return wrapped.nextGroup();
    }

    @Override
    public int group() {
        return wrapped.group();
    }

    @Override
    public void groupStats(long[] stats) {
        wrapped.groupStats(stats);
    }

    @Override
    public void close() {
        wrapped.close();
    }
}
