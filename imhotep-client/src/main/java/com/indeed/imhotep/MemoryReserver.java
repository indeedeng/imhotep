package com.indeed.imhotep;

import java.io.Closeable;

/**
 * @author jplaisance
 */
public abstract class MemoryReserver implements Closeable {

    public abstract long usedMemory();

    public abstract long totalMemory();

    public abstract boolean claimMemory(long numBytes);

    public abstract void releaseMemory(long numBytes);

    public void releaseMemory(final MemoryMeasured object) {
        final long size = object.memoryUsed();
        object.close();
        releaseMemory(size);
    }

    @Override
    public abstract void close();
}
