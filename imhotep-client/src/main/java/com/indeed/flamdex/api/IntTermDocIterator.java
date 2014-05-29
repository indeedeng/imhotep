package com.indeed.flamdex.api;

import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;

/**
 * @author jplaisance
 */
public interface IntTermDocIterator extends TermDocIterator, UnsortedIntTermDocIterator {
    long term();
    void close();
}
