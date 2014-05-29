package com.indeed.flamdex.api;

import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author jplaisance
 */
public final class GenericRawStringTermDocIterator implements RawStringTermDocIterator {

    private static final Logger log = Logger.getLogger(GenericRawStringTermDocIterator.class);

    private final RawStringTermIterator termIterator;

    private final DocIdStream docIdStream;

    public GenericRawStringTermDocIterator(RawStringTermIterator termIterator, DocIdStream docIdStream) {
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
    public byte[] termStringBytes() {
        return termIterator.termStringBytes();
    }

    @Override
    public int termStringLength() {
        return termIterator.termStringLength();
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
