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

import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author jplaisance
 */
public final class NativeDocIdStream implements DocIdStream {

    private static final Logger log = Logger.getLogger(NativeDocIdStream.class);

    private DirectMemory memory;

    private final NativeDocIdBuffer buffer = new NativeDocIdBuffer();

    private Path currentFileOpen;

    private boolean closed = false;

    private int lastDoc = 0;

    private final MapCache.Pool mapPool;

    NativeDocIdStream(MapCache.Pool mapPool) {
        this.mapPool = mapPool;
    }

    @Override
    public void reset(TermIterator term) {
        if (!(term instanceof SimpleTermIterator)) throw new IllegalArgumentException("invalid term iterator");

        try {
            internalReset((SimpleTermIterator)term);
        } catch (IOException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private void internalReset(SimpleTermIterator term) throws IOException {
        final Path filename = term.getFilename();
        if (!filename.equals(currentFileOpen)) {

            memory = mapPool.getDirectMemory(filename);
            currentFileOpen = filename;
        }
        buffer.reset(memory.getAddress()+term.getOffset(), term.docFreq());
        lastDoc = 0;
    }

    @Override
    public int fillDocIdBuffer(int[] docIdBuffer) {
        final int n = buffer.fillDocIdBuffer(docIdBuffer, docIdBuffer.length);
        for (int i = 0; i < n; i++) {
            lastDoc += docIdBuffer[i];
            docIdBuffer[i] = lastDoc;
        }
        return n;

    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            Closeables2.closeQuietly(buffer, log);
        }
    }
}
