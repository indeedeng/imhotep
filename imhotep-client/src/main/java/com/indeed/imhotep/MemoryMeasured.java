package com.indeed.imhotep;

import java.io.Closeable;

/**
 * @author jsadun
 */
public interface MemoryMeasured extends Closeable {
    public long memoryUsed();

    @Override
    public void close();
}
