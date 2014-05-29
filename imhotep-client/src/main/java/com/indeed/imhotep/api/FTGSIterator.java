package com.indeed.imhotep.api;

import java.io.Closeable;

public interface FTGSIterator extends Closeable {
    /**
     * @return true iff iterator successfully positioned to the next field
     */
    boolean nextField();

    /**
     * @return name of the current field
     */
    String fieldName();

    /**
     * @return true iff the current field is an int type
     */
    boolean fieldIsIntType();

    /**
     * @return true iff iterator successfully positioned to next term within current field
     */
    boolean nextTerm();

    /**
     * @return the number of documents to be iterated over for the current term
     */
    long termDocFreq();

    /**
     * @return the current term, if current field is an int type
     */
    long termIntVal();

    /**
     * @return the current term, if current field is a String type
     */
    String termStringVal();

    /**
     * @return true iff iterator succesfully positions to the next group within the current term
     */
    boolean nextGroup();

    /**
     * @return group id where iterator is positioned
     */
    int group();

    /**
     * @param stats array in which to store the stats associated with the current group
     */
    void groupStats(long[] stats);

    /**
     * close the iterator, this is only necessary if you want to stop the iterator before completely exhausting it
     * behavior is undefined if you call any other methods after calling close
     */
    void close();
}
