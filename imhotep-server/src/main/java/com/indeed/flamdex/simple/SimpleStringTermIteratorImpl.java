package com.indeed.flamdex.simple;

import com.google.common.base.Charsets;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.serialization.StringSerializer;
import com.indeed.lsmtree.core.Generation;
import com.indeed.lsmtree.core.ImmutableBTreeIndex;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;

/**
* @author jsgroth
*/
final class SimpleStringTermIteratorImpl implements SimpleStringTermIterator {
    private static final Logger log = Logger.getLogger(SimpleStringTermIteratorImpl.class);

    private static final int BUFFER_SIZE = 8192;

    private final byte[] buffer;
    private int bufferLen;
    private long bufferOffset;
    private int bufferPtr;

    private final String docsFilename;
    private ImmutableBTreeIndex.Reader<String, LongPair> index;
    private final File indexFile;

    private final CharsetDecoder decoder = Charsets.UTF_8.newDecoder();

    private final SharedReference<MMapBuffer> file;
    private final DirectMemory memory;

    private byte[] lastTermBytes = new byte[100];
    private ByteBuffer lastTermByteBuffer = ByteBuffer.wrap(lastTermBytes);
    private int lastTermLength = 0;
    private long lastTermOffset = 0L;
    private int lastTermDocFreq = 0;
    private String lastString = null;

    private boolean done = false;
    private boolean bufferNext = false;
    private boolean closed = false;

    SimpleStringTermIteratorImpl(MapCache mapCache, String filename, String docsFilename, String indexFilename) throws IOException {
        buffer = new byte[BUFFER_SIZE];

        this.docsFilename = docsFilename;
        final File f = new File(indexFilename);
        if (f.exists()) {
            indexFile = f;
        } else {
            indexFile = null;
        }

        file = mapCache.copyOrOpen(filename);
        memory = file.get().memory();
        done = false;
        bufferLen = 0;
        bufferOffset = 0L;
        bufferPtr = 0;
    }

    @Override
    public void reset(String term) {
        try {
            internalReset(term);
        } catch (IOException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private void internalReset(String term) throws IOException {
        if (indexFile != null) {
            if (index == null) {
                index = new ImmutableBTreeIndex.Reader<String,LongPair>(
                    indexFile,
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
            } catch (CharacterCodingException e) {
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

    private void ensureCapacity(int len) {
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
            } catch (IOException e) {
                log.error("error closing index", e);
            }
            try {
                file.close();
            } catch (IOException e) {
                log.error("error closing file", e);
            }
            closed = true;
        }
    }

    @Override
    public String getFilename() {
        return docsFilename;
    }

    @Override
    public long getOffset() {
        return lastTermOffset;
    }

    private int read() throws IOException {
        if (bufferPtr == bufferLen) {
            refillBuffer(bufferOffset + bufferLen);
            if (bufferLen == 0) return -1;
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
            ret |= ((b & 0x7F) << shift);
            if (b < 0x80) return ret;
            shift += 7;
        } while (true);
    }
}
