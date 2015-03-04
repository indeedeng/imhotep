package com.indeed.flamdex.simple;

/**
 * @author arun.
 */
class SimpleMultiShardIntTermIterator extends AbstractMultiShardTermIterator<SimpleIntTermIterator> implements MultiShardIntTermIterator {
    private long intTerm;

    SimpleMultiShardIntTermIterator(SimpleIntTermIterator[] iterators) {
        super(iterators);
    }

    @Override
    protected void recordTerm(SimpleIntTermIterator termIterator) {
        intTerm = termIterator.term();
    }

    @Override
    protected int compare(SimpleIntTermIterator a, SimpleIntTermIterator b) {
        return Long.compare(a.term(), b.term());
    }

    @Override
    public long term() {
        return intTerm;
    }
}
