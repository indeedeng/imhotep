package com.indeed.imhotep;

import com.indeed.imhotep.api.RawFTGSIterator;


/**
 * Wrapper for a RawFTGSIterator that will only return up to 'termLimit' terms that have at least 1 group.
 * Terms that don't have at least 1 non-0 group are not counted.
 * @author vladimir
 */

public class TermLimitedRawFTGSIterator extends TermLimitedFTGSIterator implements RawFTGSIterator {
    private final RawFTGSIterator wrapped;

    /**
     * @param wrapped The iterator to use
     * @param termLimit Maximum number of terms that will be allowed to iterate through
     */
    public TermLimitedRawFTGSIterator(RawFTGSIterator wrapped, long termLimit) {
        super(wrapped, termLimit);

        this.wrapped = wrapped;
    }

    @Override
    public byte[] termStringBytes() {
        return wrapped.termStringBytes();
    }

    @Override
    public int termStringLength() {
        return wrapped.termStringLength();
    }
}
