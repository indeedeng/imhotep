package com.indeed.flamdex.simple;

/**
 * @author arun.
 */
class SimpleMultiShardStringTermIterator extends AbstractMultiShardTermIterator<SimpleStringTermIterator> implements MultiShardStringTermIterator {
    private String term;

    SimpleMultiShardStringTermIterator(final SimpleStringTermIterator[] stringTermIterators) {
        super(stringTermIterators);
    }

    @Override
    protected void recordTerm(SimpleStringTermIterator termIterator) {
        term = termIterator.term();
    }

    @Override
    protected int compare(SimpleStringTermIterator a, SimpleStringTermIterator b) {
        return a.term().compareTo(b.term());
    }

    @Override
    public String term() {
        return term;
    }
}
