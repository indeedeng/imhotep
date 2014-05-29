package com.indeed.flamdex.api;

import java.io.Closeable;

/**
 * @author jplaisance
 */
public interface TermDocIterator extends Closeable {
    boolean nextTerm();
    int docFreq();
    int fillDocIdBuffer(int[] docIdBuffer);
}
