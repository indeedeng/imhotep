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

import com.indeed.flamdex.api.IntTermDocIterator;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author jplaisance
 */
public final class NativeIntTermDocIterator extends NativeTermDocIterator implements IntTermDocIterator {

    private static final Logger log = Logger.getLogger(NativeIntTermDocIterator.class);

    private final SimpleIntTermIterator termIterator;
    private long[] termBuffer = new long[128];
    private int index = 0;
    private int size = 0;

    public NativeIntTermDocIterator(SimpleIntTermIterator termIterator, MapCache mapCache)
            throws IOException {
        super(termIterator.getFilename(), mapCache);
        this.termIterator = termIterator;
    }

    @Override
    protected boolean bufferNext() {
        if (!termIterator.next()) return false;
        if (size >= termBuffer.length) {
            final long[] newBuffer = new long[termBuffer.length*2];
            System.arraycopy(termBuffer, 0, newBuffer, 0, size);
            termBuffer = newBuffer;
        }
        termBuffer[size] = termIterator.term();
        size++;
        return true;
    }

    @Override
    protected long offset() {
        return termIterator.getOffset();
    }

    @Override
    protected int lastDocFreq() {
        return termIterator.docFreq();
    }

    @Override
    protected void poll() {
        size--;
        index++;
        if (size == 0) {
            index = 0;
        }
    }

    @Override
    public long term() {
        return termBuffer[index];
    }

    @Override
    public int nextDocs(final int[] docIdBuffer) {
        return fillDocIdBuffer(docIdBuffer);
    }

    @Override
    public void close() {
        super.close();
        termIterator.close();
    }
}
