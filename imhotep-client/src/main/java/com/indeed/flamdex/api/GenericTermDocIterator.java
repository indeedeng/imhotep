package com.indeed.flamdex.api;

import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.WillCloseWhenClosed;

public class GenericTermDocIterator<I extends TermIterator> implements TermDocIterator {
    private static final Logger log = Logger.getLogger(GenericTermDocIterator.class);

    protected final I termIterator;

    private final DocIdStream docIdStream;
    private boolean resetStreamBeforeUse;

    protected GenericTermDocIterator(@WillCloseWhenClosed final I termIterator,
                                     @WillCloseWhenClosed final DocIdStream docIdStream) {
        this.termIterator = termIterator;
        this.docIdStream = docIdStream;
    }

    @Override
    public final boolean nextTerm() {
        resetStreamBeforeUse = true;
        return termIterator.next();
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
        Closeables2.closeAll(log, termIterator, docIdStream);
    }
}
