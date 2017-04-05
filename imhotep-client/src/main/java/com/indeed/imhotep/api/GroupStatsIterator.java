package com.indeed.imhotep.api;

import java.io.Closeable;

/**
 * @author aibragimov
 */

public interface GroupStatsIterator extends Closeable {
    boolean HasNext();
    long Next();
}

