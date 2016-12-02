package com.indeed.flamdex.dynamic.locks;

import java.io.Closeable;

/**
 * @author michihiko
 */

public interface MultiThreadLock extends Closeable {
    boolean isClosed();

    boolean isShared();
}

