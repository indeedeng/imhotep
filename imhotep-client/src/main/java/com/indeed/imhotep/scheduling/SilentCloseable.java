package com.indeed.imhotep.scheduling;

import java.io.Closeable;

/**
 * @author jwolfe
 */
public interface SilentCloseable extends Closeable {
    @Override
    void close();
}
