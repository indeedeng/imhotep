package com.indeed.flamdex.api;

/**
 * @author jplaisance
 */
public interface RawStringTermDocIterator extends StringTermDocIterator {

    public byte[] termStringBytes();

    public int termStringLength();
}
