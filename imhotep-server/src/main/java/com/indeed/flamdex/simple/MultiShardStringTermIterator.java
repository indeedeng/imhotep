package com.indeed.flamdex.simple;

/**
 * @author arun.
 */
public interface MultiShardStringTermIterator extends MultiShardTermIterator {
    byte[] termBytes();
    int termBytesLength();
}
