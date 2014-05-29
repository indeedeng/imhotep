package com.indeed.flamdex.fieldcache;

import java.io.Closeable;

/**
 * @author jsgroth
 *
 * A combination of {@link com.indeed.flamdex.api.IntTermIterator} and {@link com.indeed.flamdex.api.DocIdStream}
 * that does not guarantee that it will return terms in sorted order.
 *
 * Useful in the context of field caching because iterating over terms in a Lucene index in numerically sorted order
 * is very inefficient and not necessary for building a field cache.
 */
public interface UnsortedIntTermDocIterator extends Closeable {
    boolean nextTerm();
    long term();
    int nextDocs(int[] docIdBuffer);
    void close();
}
