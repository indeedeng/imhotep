package com.indeed.imhotep.io;

import com.google.common.io.ByteStreams;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author jsgroth
 */
public final class SubInputStream extends InputStream {
    private static final int INITIAL_BUFFER_SIZE = 65536;

    private final InputStream is;

    private byte[] bytes;
    private int currentIndex;
    private int currentBlockSize;
    private boolean done;

    public SubInputStream(InputStream is) {
        this.is = is;
        bytes = new byte[INITIAL_BUFFER_SIZE];
    }

    @Override
    public int read() throws IOException {
        if (done) return -1;

        if (currentIndex == currentBlockSize) {
            currentBlockSize = Streams.readInt(is);
            currentIndex = 0;
            if (currentBlockSize == -1) {
                throw new EOFException("unexpected end of stream");
            } else if (currentBlockSize == 0) {
                done = true;
                return -1;                
            }
            if (bytes.length < currentBlockSize) {
                bytes = new byte[Math.max(currentBlockSize, bytes.length*2)];
            }
            ByteStreams.readFully(is, bytes, 0, currentBlockSize);
        }
        return bytes[currentIndex++] & 0xFF;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (done) return -1;

        int count = 0;
        while (len > 0) {
            int x = read();
            if (x == -1) {
                return count;
            }

            b[off++] = (byte)x;
            --len;
            ++count;
        }

        return count;
    }
}
