package com.indeed.flamdex.simple;

import com.indeed.flamdex.api.TermIterator;

/**
 * @author jsgroth
 */
interface SimpleTermIterator extends TermIterator {
    String getFilename();
    long getOffset();
}
