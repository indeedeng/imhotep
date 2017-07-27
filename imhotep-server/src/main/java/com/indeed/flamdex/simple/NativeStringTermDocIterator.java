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

import com.google.common.base.Charsets;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author jplaisance
 */
public final class NativeStringTermDocIterator extends NativeTermDocIterator implements RawStringTermDocIterator {

    private static final Logger log = Logger.getLogger(NativeStringTermDocIterator.class);

    private final SimpleStringTermIterator termIterator;
    private byte[] buffer = new byte[4096];
    private int[] lengths = new int[128];
    private int bufferLength = 0;
    private int index = 0;
    private int position = 0;
    private int size = 0;

    private byte[] currentTermBuffer = new byte[128];

    public NativeStringTermDocIterator(final SimpleStringTermIterator termIterator, final MapCache mapCache)
            throws IOException {
        super(termIterator.getFilename(), mapCache);
        this.termIterator = termIterator;
    }

    @Override
    protected boolean bufferNext() {
        if (!termIterator.next()) {
            return false;
        }
        final int termLength = termIterator.termStringLength();
        if (bufferLength + termLength > buffer.length) {
            final byte[] newBuffer = new byte[Math.max(buffer.length*2, bufferLength + termLength)];
            System.arraycopy(buffer, 0, newBuffer, 0, bufferLength);
            buffer = newBuffer;
        }
        if (size >= lengths.length) {
            final int[] newLengths = new int[lengths.length*2];
            System.arraycopy(lengths, 0, newLengths, 0, size);
            lengths = newLengths;
        }
        lengths[size] = termLength;
        System.arraycopy(termIterator.termStringBytes(), 0, buffer, bufferLength, termLength);
        if (size == 0) {
            if (termLength > currentTermBuffer.length) {
                currentTermBuffer = new byte[Math.max(currentTermBuffer.length*2, termLength)];
            }
            System.arraycopy(termIterator.termStringBytes(), 0, currentTermBuffer, 0, termLength);
            position = termLength;
        }
        size++;
        bufferLength+=termLength;
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
            position = 0;
            bufferLength = 0;
        } else {
            if (lengths[index] > currentTermBuffer.length) {
                currentTermBuffer = new byte[Math.max(currentTermBuffer.length*2, lengths[index])];
            }
            System.arraycopy(buffer, position, currentTermBuffer, 0, lengths[index]);
            position+=lengths[index];
        }
    }

    @Override
    public String term() {
        return new String(currentTermBuffer, 0, lengths[index], Charsets.UTF_8);
    }

    @Override
    public byte[] termStringBytes() {
        return currentTermBuffer;
    }

    @Override
    public int termStringLength() {
        return lengths[index];
    }

    @Override
    public void close() {
        super.close();
        termIterator.close();
    }
}
