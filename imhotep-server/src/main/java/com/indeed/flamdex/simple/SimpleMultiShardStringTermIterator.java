package com.indeed.flamdex.simple;

import com.indeed.imhotep.RawFTGSMerger;

/**
 * @author arun.
 */
class SimpleMultiShardStringTermIterator extends AbstractMultiShardTermIterator<SimpleStringTermIterator> implements MultiShardStringTermIterator {
    private byte[] term;
    private int termLength;

    SimpleMultiShardStringTermIterator(final SimpleStringTermIterator[] stringTermIterators) {
        super(stringTermIterators);
    }

    @Override
    protected void recordTerm(SimpleStringTermIterator termIterator) {
        term = termIterator.termStringBytes();
        termLength = termIterator.termStringLength();
    }

    @Override
    protected int compare(SimpleStringTermIterator a, SimpleStringTermIterator b) {
        return RawFTGSMerger.compareBytes(a.termStringBytes(), a.termStringLength(), b.termStringBytes(), b.termStringLength());
    }

    @Override
    public byte[] termBytes() {
        return term;
    }

    @Override
    public int termBytesLength() {
        return termLength;
    }
}
