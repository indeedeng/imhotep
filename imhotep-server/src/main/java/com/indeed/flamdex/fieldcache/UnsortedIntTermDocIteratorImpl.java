package com.indeed.flamdex.fieldcache;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;

/**
 * @author jsgroth
 */
public class UnsortedIntTermDocIteratorImpl implements UnsortedIntTermDocIterator {
    private final IntTermIterator iterator;
    private final DocIdStream docIdStream;

    public UnsortedIntTermDocIteratorImpl(IntTermIterator iterator, DocIdStream docIdStream) {
        this.iterator = iterator;
        this.docIdStream = docIdStream;
    }

    public static UnsortedIntTermDocIteratorImpl create(final FlamdexReader r, final String field) {
        final IntTermIterator iterator = r.getIntTermIterator(field);
        final DocIdStream docIdStream;
        try {
            docIdStream = r.getDocIdStream();
        } catch (RuntimeException e) {
            iterator.close();
            throw e;
        }
        return new UnsortedIntTermDocIteratorImpl(iterator, docIdStream);
    }

    @Override
    public boolean nextTerm() {
        if (iterator.next()) {
            docIdStream.reset(iterator);
            return true;
        }
        return false;
    }

    @Override
    public long term() {
        return iterator.term();
    }

    @Override
    public int nextDocs(int[] docIdBuffer) {
        return docIdStream.fillDocIdBuffer(docIdBuffer);
    }

    @Override
    public void close() {
        docIdStream.close();
        iterator.close();
    }
}
