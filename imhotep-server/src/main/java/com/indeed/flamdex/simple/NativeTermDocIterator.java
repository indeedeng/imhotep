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

import com.indeed.flamdex.api.TermDocIterator;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author jplaisance
 */
public abstract class NativeTermDocIterator implements TermDocIterator {

    private static final Logger log = Logger.getLogger(NativeTermDocIterator.class);

    protected abstract boolean bufferNext();

    protected abstract long offset();

    protected abstract int lastDocFreq();

    protected abstract void poll();

    private final SharedReference<MMapBuffer> file;
    private final DirectMemory memory;

    private int bufferedTerms = 0;
    private int termIndex = 0;
    private final int[] docFreqBuffer = new int[128];

    private int currentTermDocsRemaining = 0;
    private int lastDoc = 0;

    private final NativeDocIdBuffer buffer;

    private boolean closed = false;

    NativeTermDocIterator(final Path filename, final MapCache mapCache, final boolean useSSSE3) throws IOException {
        file = mapCache.copyOrOpen(filename);
        memory = file.get().memory();
        buffer = new NativeDocIdBuffer(useSSSE3);
    }

    @Override
    public final boolean nextTerm() {
        if (currentTermDocsRemaining > 0) {
            throw new IllegalStateException("must iterate over entire doc list for previous term before calling nextTerm");
        }
        final int nextTermIndex = termIndex+1;
        if (nextTermIndex < bufferedTerms) {
            termIndex = nextTermIndex;
            currentTermDocsRemaining = docFreqBuffer[termIndex];
            poll();
            lastDoc = 0;
            return true;
        }
        if (bufferedTerms > 0) {
            poll();
        }
        long totalDocFreq = 0;
        termIndex = 0;
        bufferedTerms = 0;
        if (!bufferNext()) {
            return false;
        }
        final long offset = offset();
        do {
            docFreqBuffer[bufferedTerms] = lastDocFreq();
            totalDocFreq += lastDocFreq();
            bufferedTerms++;
        } while (bufferedTerms < docFreqBuffer.length && bufferNext());
        currentTermDocsRemaining = docFreqBuffer[termIndex];
        lastDoc = 0;
        buffer.reset(memory.getAddress()+offset, totalDocFreq);
        return true;
    }

    @Override
    public final int docFreq() {
        return docFreqBuffer[termIndex];
    }

    @Override
    public final int fillDocIdBuffer(final int[] docIdBuffer) {
        final int n = buffer.fillDocIdBuffer(docIdBuffer, Math.min(docIdBuffer.length, currentTermDocsRemaining));
        for (int i = 0; i < n; i++) {
            lastDoc += docIdBuffer[i];
            docIdBuffer[i] = lastDoc;
        }
        currentTermDocsRemaining -= n;
        return n;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            Closeables2.closeQuietly(file, log);
            Closeables2.closeQuietly(buffer, log);
        }
    }
}
