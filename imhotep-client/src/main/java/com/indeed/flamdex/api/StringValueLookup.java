package com.indeed.flamdex.api;

import com.indeed.imhotep.MemoryMeasured;

/**
 * @author jplaisance
 */
public interface StringValueLookup extends MemoryMeasured {
    String getString(int docId);
}
