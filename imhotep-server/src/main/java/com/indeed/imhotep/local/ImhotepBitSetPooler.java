package com.indeed.imhotep.local;

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;
import com.indeed.imhotep.MemoryReserver;

/**
 * @author jsgroth
 */
public class ImhotepBitSetPooler implements FastBitSetPooler {
    private final MemoryReserver memory;

    public ImhotepBitSetPooler(MemoryReserver memory) {
        this.memory = memory;
    }

    @Override
    public FastBitSet create(int size) throws FlamdexOutOfMemoryException {
        if (!memory.claimMemory(memoryUsage(size))) {
            throw new FlamdexOutOfMemoryException();
        }
        return new FastBitSet(size);
    }

    @Override
    public void release(long bytes) {
        memory.releaseMemory(bytes);
    }

    private static long memoryUsage(final int size) {
        return 8L * ((size + 64) >> 6);
    }
}
