package com.indeed.flamdex.datastruct;

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;

/**
 * @author jsgroth
 */
public interface FastBitSetPooler {
    FastBitSet create(int size) throws FlamdexOutOfMemoryException;
    void release(long bytes);
}
