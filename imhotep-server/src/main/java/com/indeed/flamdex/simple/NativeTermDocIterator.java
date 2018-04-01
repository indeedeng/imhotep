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

import javax.annotation.WillCloseWhenClosed;
import java.io.IOException;

/**
 * This class is base class to TermDocIterators that use NativeDocIdBuffer for reading docId deltas
 *
 * Class is desined based on following assumptions.
 *      1. Try to call NativeDocIdBuffer's native method on bigger chunks of data. To do so,
 *      we are caching terms, offsets and docFreqs and calculating total number of deltas that
 *      should be read by NativeDocIdBuffer. This allows us not to break on terms boundaries.
 *      2. On the other hand, we try not to do unnecessary resetting of NativeDocIdBuffer,
 *      so reset happens only when we need docId deltas from buffer, but buffer is not created yet
 *      or has wrong positioning.
 */
public abstract class NativeTermDocIterator<I extends SimpleTermIterator> implements TermDocIterator {

    private static final Logger log = Logger.getLogger(NativeTermDocIterator.class);

    // Derived classes should manage cache of terms with size BUFFER_SIZE
    protected static final int BUFFER_SIZE = 128;

    // save current term into terms buffer.
    // index - index where to save in a term buffer.
    protected abstract void cacheCurrentTerm(int index);
    // discard cache.
    protected abstract void resetCache();

    protected final I termIterator;

    // total buffered terms
    private int bufferedTermsCount = 0;
    // index of current term
    private int bufferedTermIndex = 0;
    // sum of all docFreq for buffered, but not-yet-processed terms
    private int bufferedTermsDocCount;
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
                          @WillCloseWhenClosed final I iterator,
                          final boolean useSSSE3) throws IOException {
        this.mapCache = mapCache;
        this.termIterator = iterator;
        this.useSSSE3 = useSSSE3;
    }

    @Override
    public final boolean nextTerm() {
        bufferedTermsDocCount -= docFreqBuffer[bufferedTermIndex];
        bufferedTermIndex++;

        if (bufferedTermIndex >= bufferedTermsCount) {
            // cashing new pack of terms:
            // 1. resetting cache
            // 2. buffering terms, docFreqs and offsets until end of a terms or end of buffer.
            // 3. Counting total docFreq for buffered terms.
            // 4. Updating docId stream reset params.
            resetCache();
            bufferedTermIndex = 0;
            bufferedTermsCount = 0;
            bufferedTermsDocCount = 0;
            currentTermDocsRemaining = 0;
            while ((bufferedTermsCount < BUFFER_SIZE) && termIterator.next()) {
                cacheCurrentTerm(bufferedTermsCount);
                offsets[bufferedTermsCount] = termIterator.getOffset();
                final int docFreq = termIterator.docFreq();
                docFreqBuffer[bufferedTermsCount] = docFreq;
                bufferedTermsDocCount += docFreq;
                bufferedTermsCount++;
            }

            if (bufferedTermsCount == 0) {
                // no more terms
                return false;
            }

            // params to reset docIdStream
            resetDocStream = true;
            resetPosition = offsets[bufferedTermIndex];
            resetDocCount = bufferedTermsDocCount;
        }

        if (currentTermDocsRemaining > 0) {
            // should skip some data in buffer
            if ((buffer == null) || !buffer.trySkip(currentTermDocsRemaining)) {
                // no buffer yet or can't skip, will reset on next use
                resetDocStream = true;
                resetPosition = offsets[bufferedTermIndex];
                resetDocCount = bufferedTermsDocCount;
            }
        }
        currentTermDocsRemaining = docFreqBuffer[bufferedTermIndex];
        lastDoc = 0;
        return true;
    }

    @Override
    public final int docFreq() {
        return docFreqBuffer[bufferedTermIndex];
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
            Closeables2.closeQuietly(termIterator, log);
        }
    }

    protected int getBufferedTermIndex() {
        return bufferedTermIndex;
    }

    private void prepareDocIdStream() {
        if (!resetDocStream) {
            return;
        }

        if (buffer == null) {
            try {
                file = mapCache.copyOrOpen(termIterator.getFilename());
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
