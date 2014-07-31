package com.indeed.flamdex.api;

import java.io.Closeable;

public interface TermIterator extends Closeable {
    /**
     * @return true if iterator successfully moves to the next term, terms are always traversed in ascending order
     */
    public boolean next();

    /**
     * @return how many documents are in the index that contain the current term, invalid if next() is not called or if next() returned false
     */
    public int docFreq();

    /**
     * close any resources associated with this term iterator
     */
    public void close();
}
