package com.indeed.flamdex.api;

import com.indeed.imhotep.MemoryMeasured;

public interface IntValueLookup extends MemoryMeasured {
    /**
     * @return a number less than or equal to the smallest int value in this lookup
     */
    long getMin();

    /**
     * @return a number greater than or equal to the largest int value in this lookup
     */
    long getMax();

    /**
     * @param docIds  The docIds for which to lookup values
     * @param values  The buffer in which to store retrieved values
     * @param n Only lookup values for the first n docIds
     */
    void lookup(int[] docIds, long[] values, int n);
}
