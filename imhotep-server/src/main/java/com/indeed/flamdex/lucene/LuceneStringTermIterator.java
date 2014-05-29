package com.indeed.flamdex.lucene;

import com.indeed.flamdex.api.StringTermIterator;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

import java.io.IOException;

class LuceneStringTermIterator implements StringTermIterator, LuceneTermIterator {
    private static final Logger log = Logger.getLogger(LuceneStringTermIterator.class);

    private final IndexReader reader;
    private final String field;
    private TermEnum termEnum; // if this is null it signal initialize needs to be called
    private boolean hasNext = false;
    private String firstTerm = "";

    public LuceneStringTermIterator(final IndexReader reader, final String field) {
        this.reader = reader;
        this.field = field;
    }

    @Override
    public void reset(final String term) {
        firstTerm = term;
        closeTermEnum();
    }

    private void closeTermEnum() {
        if (termEnum == null) return;
        try {
           termEnum.close();
        } catch (IOException e) {
            throw LuceneUtils.ioRuntimeException(e);
        }
        termEnum = null;
    }

    private boolean initialize() {
        try {
            termEnum = reader.terms(new Term(field, firstTerm));
        } catch (IOException e) {
            throw LuceneUtils.ioRuntimeException(e);
        }
        hasNext = termEnum.term() != null && field.equals(termEnum.term().field());
        return hasNext;
    }

    @Override
    public String term() {
        sanityCheck();
        return termEnum.term().text();
    }

    @Override
    public boolean next() {
        if (termEnum == null) {
            return initialize();
        }

        if (!hasNext) return false;

        final boolean nextSuccessful;
        try {
            nextSuccessful = termEnum.next();
        } catch (IOException e) {
            throw LuceneUtils.ioRuntimeException(e);
        }
        hasNext = nextSuccessful && termEnum.term() != null && field.equals(termEnum.term().field());
        return hasNext;
    }

    @Override
    public int docFreq() {
        sanityCheck();
        return termEnum.docFreq();
    }

    @Override
    public void close() {
        if (termEnum != null) {
            try {
                closeTermEnum();
            } catch (RuntimeException e) {
                log.error("error closing TermEnum", e);
            }
        }
    }

    @Override
    public TermEnum termEnum() {
        sanityCheck();
        return termEnum;
    }

    private void sanityCheck() {
        if (!hasNext) {
            throw new IllegalArgumentException("Invalid operation given iterators current state");
        }
    }
}
