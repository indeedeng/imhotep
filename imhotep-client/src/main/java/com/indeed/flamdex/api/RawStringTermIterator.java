package com.indeed.flamdex.api;

/**
 * @author jplaisance
 */
public interface RawStringTermIterator extends StringTermIterator {

    public byte[] termStringBytes();

    public int termStringLength();
}
