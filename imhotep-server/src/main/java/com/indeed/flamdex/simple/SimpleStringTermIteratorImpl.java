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
import com.indeed.lsmtree.core.Generation;
import com.indeed.lsmtree.core.ImmutableBTreeIndex;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import com.indeed.util.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Path;
import java.util.Arrays;

/**
* @author jsgroth
*/
final class SimpleStringTermIteratorImpl implements SimpleStringTermIterator {
    private static final Logger log = Logger.getLogger(SimpleStringTermIteratorImpl.class);

    private static final int BUFFER_SIZE = 8192;

    private final MapCache mapCache;

    private final byte[] buffer;
    private long docListAddress = 0L;
    private int bufferLen;
    private long bufferOffset;
    private int bufferPtr;

    private final Path docListPath;
    private ImmutableBTreeIndex.Reader<String, LongPair> index;
    private final Path indexPath;

    private final CharsetDecoder decoder = Charsets.UTF_8.newDecoder();

    private final SharedReference<MMapBuffer> file;
    private final DirectMemory memory;

    private SharedReference<MMapBuffer> docListFile = null;

    private byte[] lastTermBytes = new byte[100];
    private ByteBuffer lastTermByteBuffer = ByteBuffer.wrap(lastTermBytes);
    private int lastTermLength = 0;
    private long lastTermOffset = 0L;
    private int lastTermDocFreq = 0;
    private String lastString = null;

    private boolean done = false;
    private boolean bufferNext = false;
    private boolean closed = false;

    SimpleStringTermIteratorImpl(
            final MapCache mapCache,
            final Path filename,
            final Path docListPath,
            final Path indexPath) throws IOException {
        this.mapCache = mapCache;

        buffer = new byte[BUFFER_SIZE];

        this.docListPath = docListPath;
        this.indexPath = indexPath;

        file = mapCache.copyOrOpen(filename);
        memory = file.get().memory();
        done = false;
        bufferLen = 0;
        bufferOffset = 0L;
        bufferPtr = 0;
    }

    @Override
    public void reset(final String term) {
        try {
            internalReset(term);
        } catch (final IOException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private void internalReset(final String term) throws IOException {
        if (indexPath != null) {
            if (index == null) {
                index = new ImmutableBTreeIndex.Reader<>(indexPath,
                    new StringSerializer(),
                    new LongPairSerializer(),
                    false
                );
            }
            Generation.Entry<String, LongPair> e = index.floor(term);
            if (e == null) {
                e = index.first();
            }
            lastTermBytes = e.getKey().getBytes(Charsets.UTF_8);
            lastTermByteBuffer = ByteBuffer.wrap(lastTermBytes);
            lastTermLength = lastTermBytes.length;
            lastString = null;
            final LongPair p = e.getValue();
            refillBuffer(p.getFirst());
            lastTermOffset = p.getSecond();
            lastTermDocFreq = (int)readVLong();
            done = false;

            while (decoder.decode((ByteBuffer)lastTermByteBuffer.position(0).limit(lastTermLength)).toString().compareTo(term) < 0 && next()) {}
            bufferNext = true;
        } else {
            lastTermLength = 0;
            lastTermOffset = 0L;
            lastTermDocFreq = 0;
            lastString = null;

            bufferLen = 0;
            bufferOffset = 0L;
            bufferPtr = 0;

            done = false;

            while (next() && new String(lastTermBytes, 0, lastTermLength, Charsets.UTF_8).compareTo(term) < 0) {}
            bufferNext = true;
        }
    }

    @Override
    public String term() {
        if (lastString == null) {
            try {
                lastString = decoder.decode((ByteBuffer)lastTermByteBuffer.position(0).limit(lastTermLength)).toString();
            } catch (final CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
        return lastString;
    }

    @Override
    public byte[] termStringBytes() {
        return lastTermBytes;
    }

    @Override
    public int termStringLength() {
        return lastTermLength;
    }

    @Override
    public boolean next() {
        try {
            return internalNext();
        } catch (final IOException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private boolean internalNext() throws IOException {
        if (done) {
            return false;
        }
        if (bufferNext) {
            bufferNext = false;
            return true;
        }

        final int firstByte = read();
        if (firstByte == -1) {
            done = true;
            return false;
        }

        final int removeLen = (int)readVLong(firstByte);
        final int newLen = (int)readVLong();

        ensureCapacity(lastTermLength - removeLen + newLen);
        readFully(lastTermBytes, lastTermLength - removeLen, newLen);
        lastTermLength = lastTermLength - removeLen + newLen;
        lastString = null;

        final long offsetDelta = readVLong();
        lastTermOffset += offsetDelta;

        lastTermDocFreq = (int)readVLong();

        return true;
    }

    private void ensureCapacity(final int len) {
        // TODO is > sufficient here? I think yes, verify later
        if (len >= lastTermBytes.length) {
            lastTermBytes = Arrays.copyOf(lastTermBytes, Math.max(len, 2*lastTermBytes.length));
            lastTermByteBuffer = ByteBuffer.wrap(lastTermBytes);
        }
    }

    @Override
    public int docFreq() {
        return lastTermDocFreq;
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                if (index != null) {
                    index.close();
                }
            } catch (final IOException e) {
                log.error("error closing index", e);
            }
            try {
                file.close();
            } catch (final IOException e) {
                log.error("error closing file", e);
            }
            try {
                if (docListFile != null) {
                    docListFile.close();
                }
            } catch (final IOException e) {
                log.error("error closing docListFile", e);
            }
            closed = true;
        }
    }

    @Override
    public Path getFilename() {
        return docListPath;
    }

    @Override
    public long getOffset() {
        return lastTermOffset;
    }

    @Override
    public long getDocListAddress()
        throws IOException {
        if (docListFile == null) {
            docListFile = mapCache.copyOrOpen(docListPath);
            docListAddress = docListFile.get().memory().getAddress();
        }
        return this.docListAddress;
    }

    private int read() throws IOException {
        if (bufferPtr == bufferLen) {
            refillBuffer(bufferOffset + bufferLen);
            if (bufferLen == 0) {
                return -1;
            }
        }
        return buffer[bufferPtr++] & 0xFF;
    }

    private void readFully(final byte[] b, int off, int len) throws IOException {
        while (true) {
            final int available = bufferLen - bufferPtr;
            if (available >= len) {
                System.arraycopy(buffer, bufferPtr, b, off, len);
                bufferPtr += len;
                return;
            } else {
                System.arraycopy(buffer, bufferPtr, b, off, available);
                off += available;
                len -= available;
                refillBuffer(bufferOffset + bufferLen);
            }
        }
    }

    private void refillBuffer(final long offset) throws IOException {
        bufferLen = (int)Math.min(buffer.length, memory.length() - offset);
        if (bufferLen > 0) {
            memory.getBytes(offset, buffer, 0, bufferLen);
        }
        bufferOffset = offset;
        bufferPtr = 0;
    }

    private long readVLong(int b) throws IOException {
        long ret = 0L;
        int shift = 0;
        do {
            ret |= ((b & 0x7F) << shift);
            if (b < 0x80) {
                return ret;
            }
            shift += 7;
            b = read();
        } while (true);
    }

    private long readVLong() throws IOException {
        long ret = 0L;
        int shift = 0;
        do {
            final int b = read();
            ret |= ((b & 0x7F) << shift);
            if (b < 0x80) {
                return ret;
            }
            shift += 7;
        } while (true);
    }
}
