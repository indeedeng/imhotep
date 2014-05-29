package com.indeed.imhotep.api;

/**
 * @author jsgroth
 *
 * an FTGS iterator that supports accessing the raw UTF-8 byte array for the current string term
 */
public interface RawFTGSIterator extends FTGSIterator {
    /**
     * This method returns the UTF-8 bytes for the current string term
     * It will generally return a reference to an internal array instead of a new copy, so if you're going to advance the
     * iterator before reading the bytes then you need to copy them into another array before calling any of the next* methods
     *
     * @return a reference to an internal byte array
     */
    byte[] termStringBytes();

    /**
     * @return the number of bytes in the return value of {@link #termStringBytes()}} used for the current term
     */
    int termStringLength();
}
