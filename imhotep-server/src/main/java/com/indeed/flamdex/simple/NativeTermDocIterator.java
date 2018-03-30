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

import com.google.common.base.Throwables;
import com.indeed.flamdex.api.TermDocIterator;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author jplaisance
 */
public abstract class NativeTermDocIterator implements TermDocIterator {

    private static final Logger log = Logger.getLogger(NativeTermDocIterator.class);

    // Derived classes should manage cache of terms with size BUFFER_SIZE
    protected static final int BUFFER_SIZE = 128;

    // cache current term into buffer.
    // @param index - index of cached term in buffer
    protected abstract void cacheTerm(int index);
    // discard cache.
    protected abstract void resetCache();

    private final SimpleTermIterator iterator;

    // total buffered terms
    private int bufferedTerms = 0;
    // index of current term
    private int termIndex = 0;
    // sum of all docFreq for buffered, but not-yet-processed terms
    private int cachedTermsDocCount;
    // docFreq's for buffered terms
    private final int[] docFreqBuffer = new int[BUFFER_SIZE];
    // offsets for buffered terms
    private final long[] offsets = new long[BUFFER_SIZE];

    // state of current term docIdStream.
    private int currentTermDocsRemaining = 0;
    private int lastDoc = 0;

    // created on first use.
    private NativeDocIdBuffer buffer;

    // All these we need for NativeDocIdBuffer
    private final MapCache mapCache;
    private SharedReference<MMapBuffer> file;
    private DirectMemory memory;
    private final boolean useSSSE3;

    // Should we reset docIdStream before next use and params for resetting.
    private boolean resetDocStream;
    private long resetPosition;
    private long resetDocCount;

    private boolean closed = false;

    NativeTermDocIterator(final MapCache mapCache,
                          final SimpleTermIterator iterator,
                          final boolean useSSSE3) throws IOException {
        this.mapCache = mapCache;
        this.iterator = iterator;
        this.useSSSE3 = useSSSE3;
    }

    @Override
    public final boolean nextTerm() {
        cachedTermsDocCount -= docFreqBuffer[termIndex];
        termIndex++;

        if (termIndex >= bufferedTerms) {
            // try to cache terms
            resetCache();
            termIndex = 0;
            bufferedTerms = 0;
            cachedTermsDocCount = 0;
            currentTermDocsRemaining = 0;
            while ((bufferedTerms < BUFFER_SIZE) && iterator.next()) {
                cacheTerm(bufferedTerms);
                offsets[bufferedTerms] = iterator.getOffset();
                final int docFreq = iterator.docFreq();
                docFreqBuffer[bufferedTerms] = docFreq;
                cachedTermsDocCount += docFreq;
                bufferedTerms++;
            }

            if (bufferedTerms == 0) {
                // no more terms
                return false;
            }

            // need to reset docIdStream
            resetDocStream = true;
            resetPosition = offsets[termIndex];
            resetDocCount = cachedTermsDocCount;
        }

        if (currentTermDocsRemaining > 0) {
            // should skip some data in buffer
            if ((buffer == null) || !buffer.trySkip(currentTermDocsRemaining)) {
                // no buffer yet or can't skip, will reset on next use
                resetDocStream = true;
                resetPosition = offsets[termIndex];
                resetDocCount = cachedTermsDocCount;
            }
        }
        currentTermDocsRemaining = docFreqBuffer[termIndex];
        lastDoc = 0;
        return true;
    }

    @Override
    public final int docFreq() {
        return docFreqBuffer[termIndex];
    }

    @Override
    public final int fillDocIdBuffer(final int[] docIdBuffer) {
        prepareDocIdStream();
        final int n = buffer.fillDocIdBuffer(docIdBuffer, Math.min(docIdBuffer.length, currentTermDocsRemaining));
        for (int i = 0; i < n; i++) {
            lastDoc += docIdBuffer[i];
            docIdBuffer[i] = lastDoc;
        }
        currentTermDocsRemaining -= n;
        return n;
    }

    @Override
    public final void close() {
        if (!closed) {
            closed = true;
            Closeables2.closeQuietly(file, log);
            Closeables2.closeQuietly(buffer, log);
            Closeables2.closeQuietly(iterator, log);
        }
    }

    protected int getTermIndex() {
        return termIndex;
    }

    private void prepareDocIdStream() {
        if (!resetDocStream) {
            return;
        }

        if (buffer == null) {
            try {
                file = mapCache.copyOrOpen(iterator.getFilename());
                memory = file.get().memory();
                buffer = new NativeDocIdBuffer(useSSSE3);
            } catch (final IOException e) {
                throw Throwables.propagate(e);
            }
        }

        buffer.reset(memory.getAddress() + resetPosition, resetDocCount);
        resetDocStream = false;
    }
}
