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

import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.lsmtree.core.Generation;
import com.indeed.lsmtree.core.ImmutableBTreeIndex;
import com.indeed.util.mmap.DirectMemory;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;

/**
* @author jsgroth
*/
final class SimpleIntTermIteratorImpl implements SimpleIntTermIterator {
    private static final Logger log = Logger.getLogger(SimpleIntTermIteratorImpl.class);

    private static final int BUFFER_SIZE = 8192;

    private final MapCache.Pool mapPool;

    private final byte[] buffer;
    private long docListAddress = 0L;
    private int bufferLen;
    private long bufferOffset;
    private int bufferPtr;

    private final Path filename;
    private final Path docListPath;
    private ImmutableBTreeIndex.Reader<Integer, LongPair> index;
    private ImmutableBTreeIndex.Reader<Long, LongPair> index64;
    private final Path indexPath;
    private final boolean use64BitIndex;

    private final DirectMemory memory;
    private DirectMemory docListMem = null;

    private long lastTerm = 0;
    private long lastTermOffset = 0L;
    private int lastTermDocFreq = 0;

    private boolean done = false;
    private boolean bufferNext = false;
    private boolean closed = false;

    SimpleIntTermIteratorImpl(MapCache.Pool mapPool, Path filename, Path docListPath, Path indexPath)
        throws IOException {

        this.mapPool = mapPool;

        buffer = new byte[BUFFER_SIZE];

        this.filename = filename;
        this.docListPath = docListPath;
        this.indexPath = indexPath;

        if (indexPath != null) {
            if (indexPath.endsWith(".intindex64")) {
                this.use64BitIndex = true;
            } else if (indexPath.endsWith(".intindex")) {
                this.use64BitIndex = false;
            } else {
                throw new RuntimeException(
                        "Invalid index type: " + indexPath.getFileName().toString());
            }
        } else {
            this.use64BitIndex = true;
        }

        memory = mapPool.getDirectMemory(filename);

        done = false;
        bufferLen = 0;
        bufferOffset = 0L;
        bufferPtr = 0;
    }

    @Override
    public void reset(long term) {
        try {
            internalReset(term);
        } catch (IOException e) {
            close();
            throw new RuntimeException(e);
        } catch (IndexOutOfBoundsException e) {
            close();
            log.error("IIOB: filename="+filename, e);
            throw e;
        }
    }

    public void internalReset(long term) throws IOException {
        if (indexPath != null) {
            final LongPair p;
            if (use64BitIndex) {
                if (index64 == null) {
                    index64 = new ImmutableBTreeIndex.Reader<Long,LongPair>(indexPath,
                            new LongSerializer(),
                            new LongPairSerializer(),
                            false
                    );
                }
                Generation.Entry<Long, LongPair> e = index64.floor(term);
                if (e == null) {
                    e = index64.first();
                }
                lastTerm = e.getKey();
                p = e.getValue();
            } else {
                if (index == null) {
                    index = new ImmutableBTreeIndex.Reader<Integer,LongPair>(indexPath,
                            new IntSerializer(),
                            new LongPairSerializer(),
                            false
                    );
                }
                Generation.Entry<Integer, LongPair> e = index.floor((int)term);
                if (e == null) {
                    e = index.first();
                }
                lastTerm = e.getKey();
                p = e.getValue();
            }

            refillBuffer(p.getFirst());
            lastTermOffset = p.getSecond();
            lastTermDocFreq = (int)readVLong();
            done = false;

            while (lastTerm < term && next()) {}
            bufferNext = true;
        } else {
            lastTerm = 0;
            lastTermOffset = 0L;
            lastTermDocFreq = 0;

            bufferLen = 0;
            bufferOffset = 0L;
            bufferPtr = 0;

            done = false;

            while (next() && lastTerm < term) {}
            bufferNext = true;
        }
    }

    @Override
    public long term() {
        return lastTerm;
    }

    @Override
    public boolean next() {
        try {
            return internalNext();
        } catch (IOException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private boolean internalNext() throws IOException {
        if (done) return false;
        if (bufferNext) {
            bufferNext = false;
            return true;
        }

        final int firstByte = read();
        if (firstByte == -1) {
            done = true;
            return false;
        }

        final long termDelta = readVLong(firstByte);
        lastTerm += termDelta;

        final long offsetDelta = readVLong();
        lastTermOffset += offsetDelta;

        lastTermDocFreq = (int)readVLong();

        return true;
    }

    @Override
    public int docFreq() {
        return lastTermDocFreq;
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                if (index64 != null) {
                    index64.close();
                }
                if (index != null) {
                    index.close();
                }
            } catch (IOException e) {
                log.error("error closing index", e);
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
    public long getDocListAddress() throws IOException {
        if (docListMem == null) {
            docListMem = mapPool.getDirectMemory(docListPath);
            docListAddress = docListMem.getAddress();
        }
        return this.docListAddress;
    }

    private int read() throws IOException {
        if (bufferPtr == bufferLen) {
            refillBuffer(bufferOffset + bufferLen);
            if (bufferLen == 0) return -1;
        }
        return buffer[bufferPtr++] & 0xFF;
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
            ret |= ((b & 0x7FL) << shift);
            if (b < 0x80) return ret;
            shift += 7;
            b = read();
        } while (true);
    }

    private long readVLong() throws IOException {
        long ret = 0L;
        int shift = 0;
        do {
            int b = read();
            ret |= ((b & 0x7FL) << shift);
            if (b < 0x80) return ret;
            shift += 7;
        } while (true);
    }
}
