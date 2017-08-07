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

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.TermIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * @author jsgroth
 */
abstract class SimpleDocIdStream implements DocIdStream {
    private static final Logger log = Logger.getLogger(SimpleDocIdStream.class);

    protected final byte[] buffer;
    protected final ByteBuffer wrappedBuffer;
    protected int bufferLen;

    private long bufferOffset;
    private int bufferPtr;

    private int docsRemaining;
    private int lastDoc;

    private Path currentFileOpen;

    public SimpleDocIdStream(final byte[] buffer) {
        this.buffer = buffer;
        this.wrappedBuffer = ByteBuffer.wrap(buffer);

        bufferOffset = 0L;
        bufferLen = 0;
        bufferPtr = 0;
    }

    @Override
    public void reset(final TermIterator term) {
        if (!(term instanceof SimpleTermIterator)) {
            throw new IllegalArgumentException("invalid term iterator");
        }

        try {
            internalReset((SimpleTermIterator)term);
        } catch (final IOException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private void internalReset(final SimpleTermIterator term) throws IOException {
        final Path filename = term.getFilename();
        if (!filename.equals(currentFileOpen)) {

            openFile(filename);

            currentFileOpen = filename;
            // to force a refill
            bufferOffset = 0L;
            bufferLen = 0;
            bufferPtr = 0;
        }

        final long offset = term.getOffset();
        if (offset >= bufferOffset && offset < bufferOffset + bufferLen) {
            bufferPtr = (int) (offset - bufferOffset);
        } else {
            refillBuffer(offset);
        }

        docsRemaining = term.docFreq();
        lastDoc = 0;
    }

    @Override
    public int fillDocIdBuffer(final int[] docIdBuffer) {
        if (docsRemaining == 0) {
            return 0;
        }

        try {
            final int n = Math.min(docsRemaining, docIdBuffer.length);
            for (int i = 0; i < n; ++i) {
                final int docDelta = readVInt();
                lastDoc += docDelta;
                docIdBuffer[i] = lastDoc;
            }
            docsRemaining -= n;
            return n;
        } catch (final IOException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private int readVInt() throws IOException {
        int ret = 0;
        int shift = 0;
        do {
            if (bufferPtr == bufferLen) {
                refillBuffer(bufferOffset + bufferLen);
            }
            final byte b = buffer[bufferPtr];
            bufferPtr++;
            ret |= ((b & 0x7F) << shift);
            if (b >= 0) {
                return ret;
            }
            shift += 7;
        } while (true);
    }

    private void refillBuffer(final long offset) throws IOException {
        bufferLen = (int)Math.min(buffer.length, getLength() - offset);
        if (bufferLen > 0) {
            readBytes(offset);
        }
        bufferOffset = offset;
        bufferPtr = 0;
    }

    protected abstract void openFile(Path filePath) throws IOException;
    protected abstract long getLength();
    protected abstract void readBytes(long offset);
    public abstract void close();
}
