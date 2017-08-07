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

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.TermIterator;
import org.apache.lucene.index.TermDocs;

import java.io.IOException;

public class LuceneDocIdStream implements DocIdStream {
    private final TermDocs termDocs;
    private boolean valid = false;

    public LuceneDocIdStream(final TermDocs termDocs) {
        this.termDocs = termDocs;
    }

    @Override
    public void reset(final TermIterator term) {
        if (!(term instanceof LuceneTermIterator)) {
            throw new IllegalArgumentException("This DocIdStream can only function with LuceneTermIterator");
        }
        internalReset((LuceneTermIterator)term);
    }

    private void internalReset(final LuceneTermIterator term) {
        try {
            termDocs.seek(term.termEnum());
        } catch (final IOException e) {
            throw e(e);
        }
        valid = true;
    }

    @Override
    public int fillDocIdBuffer(final int[] docIdBuffer) {
        if (!valid) {
            throw new IllegalArgumentException("DocIdStream is not in a valid state");
        }
        try {
            return internalFillDocIdBuffer(docIdBuffer);
        } catch (final IOException e) {
            throw e(e);
        }
    }

    private int internalFillDocIdBuffer(final int[] docIdBuffer) throws IOException {
        int i = 0;
        for (; i < docIdBuffer.length; i++) {
            if (!termDocs.next()) {
                break; // todo: think about setting valid = false
            }
            docIdBuffer[i] = termDocs.doc();
        }
        return i;
    }

    @Override
    public void close() {
        try {
            termDocs.close();
        } catch (final IOException e) {
            throw e(e);
        }
    }

    private RuntimeException e(final IOException e) {
        valid = false;
        throw LuceneUtils.ioRuntimeException(e);
    }
}
