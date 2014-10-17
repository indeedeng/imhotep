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
 package com.indeed.flamdex.simple;

import com.indeed.flamdex.utils.FlamdexUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author jsgroth
 */
abstract class SimpleFieldWriter {
    protected final OutputStream termsOutput;
    protected final OutputStream docsOutput;
    protected final long numDocs;

    private long lastOffsetWritten = 0L;

    private long currentTermOffset = 0L;
    private long currentTermDocFreq = 0L;

    private long lastDocWritten = 0L;
    private long docsBytesWritten = 0L;

    private boolean nextTermCalled = false;
    private boolean nextDocCalled = false;

    protected SimpleFieldWriter(OutputStream termsOutput, OutputStream docsOutput, long numDocs) {
        this.termsOutput = termsOutput;
        this.docsOutput = docsOutput;
        this.numDocs = numDocs;
    }

    protected void internalNextTerm() throws IOException {
        writeTerm();
        nextTermCalled = true;
        currentTermOffset = docsBytesWritten;
        currentTermDocFreq = 0L;
        lastDocWritten = 0L;
        nextDocCalled = false;
    }

    /**
     * add a document for the current term
     *
     * @param doc the doc to add
     * @throws IOException if there is a file write error
     * @throws IllegalStateException if called before nextTerm
     * @throws IllegalArgumentException if doc is greater than or equal to the number of documents in the index,
     *                                  if doc is negative, or if doc is less than or equal to the previous doc added
     */
    public void nextDoc(int doc) throws IOException {
        if (!nextTermCalled) throw new IllegalStateException("nextTerm must be called before nextDoc");
        if (doc < 0) throw new IllegalArgumentException("doc cannot be negative");
        if (doc >= numDocs) throw new IllegalArgumentException("doc is >= maxDoc: doc="+doc+", maxDoc="+ numDocs);

        if (nextDocCalled && doc <= lastDocWritten) {
            throw new IllegalArgumentException("docs must be in sorted order: "+doc+" is not greater than "+lastDocWritten);
        }
        nextDocCalled = true;

        final long docDelta = doc - lastDocWritten;
        docsBytesWritten += FlamdexUtils.writeVLong(docDelta, docsOutput);
        lastDocWritten = doc;
        ++currentTermDocFreq;
    }

    protected void writeTerm() throws IOException {
        if (currentTermDocFreq == 0) return;

        writeTermDelta();

        final long offsetDelta = currentTermOffset - lastOffsetWritten;
        FlamdexUtils.writeVLong(offsetDelta, termsOutput);
        lastOffsetWritten = currentTermOffset;

        FlamdexUtils.writeVLong(currentTermDocFreq, termsOutput);
    }

    protected abstract void writeTermDelta() throws IOException;

    protected abstract void writeBTreeIndex() throws IOException;

    public void close() throws IOException {
        writeTerm();
        termsOutput.close();
        docsOutput.close();
        if (nextTermCalled) {
            writeBTreeIndex();
        }
    }
}
