package com.indeed.flamdex.api;

/**
 * @author jplaisance
 */
public interface RawFlamdexReader extends FlamdexReader {

    @Override
    RawStringTermIterator getStringTermIterator(String field);

    @Override
    RawStringTermDocIterator getStringTermDocIterator(String field);
}
