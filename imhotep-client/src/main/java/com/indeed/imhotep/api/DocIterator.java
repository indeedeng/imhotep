package com.indeed.imhotep.api;

import java.io.Closeable;

/**
 * @author jplaisance
 */
public interface DocIterator extends Closeable {
    public boolean next();
    public int getGroup();
    public long getInt(int index);
    public String getString(int index);
}
