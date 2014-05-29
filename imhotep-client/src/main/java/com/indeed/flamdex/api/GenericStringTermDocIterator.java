package com.indeed.flamdex.api;

import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author jplaisance
 */
public final class GenericStringTermDocIterator implements StringTermDocIterator {

    private static final Logger log = Logger.getLogger(GenericStringTermDocIterator.class);

    private final StringTermIterator termIterator;

    private final DocIdStream docIdStream;

    public GenericStringTermDocIterator(StringTermIterator termIterator, DocIdStream docIdStream) {
        this.termIterator = termIterator;
        this.docIdStream = docIdStream;
    }

    @Override
    public boolean nextTerm() {
        final boolean ret = termIterator.next();
        if (ret) {
            docIdStream.reset(termIterator);
        }
        return ret;
    }

    @Override
    public String term() {
        return termIterator.term();
    }

    @Override
    public int docFreq() {
        return termIterator.docFreq();
    }

    @Override
    public int fillDocIdBuffer(final int[] docIdBuffer) {
        return docIdStream.fillDocIdBuffer(docIdBuffer);
    }

    @Override
    public void close() throws IOException {
        termIterator.close();
        docIdStream.close();
    }
}
