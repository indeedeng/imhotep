package com.indeed.flamdex.api;

public interface StringTermIterator extends TermIterator {
    /**
     * Resets this iterator, so that the next time next() is called it will be positioned at the first term that is >= provided term.  The iterator is
     * no longer valid until the next call to next()
     * @param term The term to reset the iterator to
     */
    public void reset(String term);

    /**
     * @return  the current term, invalid before next() is called or if next() returned false
     */
    public String term();
}