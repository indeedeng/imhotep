/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.flamdex.lucene;

import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

import java.io.IOException;

/**
 * @author jsgroth
 */
final class LuceneUnsortedIntTermDocIterator implements UnsortedIntTermDocIterator {
    private static final Logger log = Logger.getLogger(LuceneUnsortedIntTermDocIterator.class);

    private final String field;
    private final TermEnum terms;
    private final TermDocs termDocs;

    private long currentTerm;
    private boolean first = true;

    LuceneUnsortedIntTermDocIterator(final String field, final TermEnum terms, final TermDocs termDocs) {
        this.field = field.intern();
        this.terms = terms;
        this.termDocs = termDocs;
    }

    static LuceneUnsortedIntTermDocIterator create(final IndexReader r, final String field) throws IOException {
        final TermEnum terms = r.terms(new Term(field, ""));
        final TermDocs termDocs;
        try {
            termDocs = r.termDocs();
        } catch (final IOException e) {
            try {
                terms.close();
            } catch (final IOException e1) {
                log.error("error closing TermEnum", e1);
            }
            throw e;
        }
        return new LuceneUnsortedIntTermDocIterator(field, terms, termDocs);
    }

    @Override
    public boolean nextTerm() {
        try {
            return innerNextTerm();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"StringEquality"})
    private boolean innerNextTerm() throws IOException {
        // loop instead of calling itself recursively to avoid potential stack overflow
        while (true) {
            if (!first) {
                if (!terms.next()) {
                    return false;
                }
            } else {
                first = false;
            }
            final Term term = terms.term();
            if (term == null || term.field() != field) {
                return false;
            }

            try {
                currentTerm = Long.parseLong(term.text());
            } catch (final NumberFormatException e) {
                continue;
            }
            
            termDocs.seek(terms);

            return true;
        }
    }

    @Override
    public long term() {
        return currentTerm;
    }

    @Override
    public int docFreq() {
        return termDocs.freq();
    }

    @Override
    public int nextDocs(final int[] docIdBuffer) {
        try {
            return innerNextDocs(docIdBuffer);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int innerNextDocs(final int[] docIdBuffer) throws IOException {
        int i = 0;
        while (i < docIdBuffer.length && termDocs.next()) {
            docIdBuffer[i] = termDocs.doc();
            i++;
        }
        return i;
    }

    @Override
    public void close() {
        try {
            terms.close();
        } catch (final IOException e) {
            log.error("error closing TermEnum", e);
        }
        try {
            termDocs.close();
        } catch (final IOException e) {
            log.error("error closing TermDocs", e);
        }
    }
}
