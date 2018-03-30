package com.indeed.flamdex.api;

import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.util.Arrays;

public class GenericTermDocIterator implements TermDocIterator {
    private static final Logger log = Logger.getLogger(GenericTermDocIterator.class);

    private final TermIterator termIterator;

    private final DocIdStream docIdStream;
    private boolean resetStreamBeforeUse;

    protected GenericTermDocIterator(final TermIterator termIterator, final DocIdStream docIdStream) {
        this.termIterator = termIterator;
        this.docIdStream = docIdStream;
    }

    @Override
    public final boolean nextTerm() {
        final boolean ret = termIterator.next();
        resetStreamBeforeUse = ret;
        return ret;
    }

    @Override
    public final int docFreq() {
        return termIterator.docFreq();
    }

    @Override
    public final int fillDocIdBuffer(final int[] docIdBuffer) {
        if (resetStreamBeforeUse) {
            docIdStream.reset(termIterator);
            resetStreamBeforeUse = false;
        }
        return docIdStream.fillDocIdBuffer(docIdBuffer);
    }

    @Override
    public final void close() {
        Closeables2.closeAll(Arrays.asList(termIterator, docIdStream), log);
    }
}
