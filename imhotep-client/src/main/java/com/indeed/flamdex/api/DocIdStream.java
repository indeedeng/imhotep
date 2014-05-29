package com.indeed.flamdex.api;

public interface DocIdStream {
    /**
     * Resets this DocIdStream to start streaming out docIds for the specified term
     * @param term The iterator pointing to the term of interest, iterator must be in a valid state 
     */
    void reset(TermIterator term);

    /**
     * @param docIdBuffer the buffer into which to put docIds
     * @return  How many docIds were put into the buffer, if this is less than the length of the buffer, the Stream is considered finished
     */
    int fillDocIdBuffer(int[] docIdBuffer);

    /**
     * closes any open resources
     */
    void close();
}
