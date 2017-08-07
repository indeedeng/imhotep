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
    private TermEnum termEnum; // if this is null it signals initialize needs to be called
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
        if (termEnum == null) {
            return;
        }
        try {
           termEnum.close();
        } catch (final IOException e) {
            throw LuceneUtils.ioRuntimeException(e);
        }
        termEnum = null;
    }

    private boolean initialize() {
        try {
            termEnum = reader.terms(new Term(field, firstTerm));
        } catch (final IOException e) {
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

        if (!hasNext) {
            return false;
        }

        final boolean nextSuccessful;
        try {
            nextSuccessful = termEnum.next();
        } catch (final IOException e) {
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
            } catch (final RuntimeException e) {
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
