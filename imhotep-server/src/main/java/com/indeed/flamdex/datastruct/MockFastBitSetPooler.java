package com.indeed.flamdex.datastruct;

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;

/**
 * @author jsgroth
 */
public class MockFastBitSetPooler implements FastBitSetPooler {
    @Override
    public FastBitSet create(int size) throws FlamdexOutOfMemoryException {
        return new FastBitSet(size);
    }

    @Override
    public void release(long bytes) {
    }
}
