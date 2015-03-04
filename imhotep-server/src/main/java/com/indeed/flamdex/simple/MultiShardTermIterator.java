package com.indeed.flamdex.simple;

import java.io.Closeable;

/**
 * An interface to iterate all terms in a set of a shards
 * @author arun.
 */
public interface MultiShardTermIterator extends Closeable {
    boolean next();
    /**
     * fills the buffer with the current term's offset in each shard. The length of <code>buffer</code> must
     * be equal to the number of shards
     */
    void offsets(long[] buffer);
}
