package com.indeed.flamdex.search;

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;

/**
 * @author jsgroth
 */
interface QueryEvaluator {
    void and(FlamdexReader r, FastBitSet bitSet, FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException;
    void or(FlamdexReader r, FastBitSet bitSet, FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException;
    void not(FlamdexReader r, FastBitSet bitSet, FastBitSetPooler bitSetPooler) throws FlamdexOutOfMemoryException;
}
