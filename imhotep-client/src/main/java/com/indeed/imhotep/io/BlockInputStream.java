package com.indeed.imhotep.io;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

import javax.annotation.Nonnull;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author xweng
 *
 * A wrapped input stream to read data written by {@link BlockOutputStream}. It continuously reads data until the end of stream.
 *
 * This stream separates the data into several blocks and send them out. The format of each block is:
 * -- 4 bytes --  -- block size bytes --
 *  [block size]      [actual data]
 * At the end of stream, it has an empty block with 0 block size, which indicates no following blocks will come and the stream ends.
 *
 * You need to read through the end of the block input stream, or otherwise the original input stream will contain garbage.
 */
@NotThreadSafe
public class BlockInputStream extends FilterInputStream {
    private int count;
    private int pos;

    private boolean hasNext;
    private boolean closed;

    private final byte[] blockSizeBytes = new byte[4];

    /**
     * Initialize a block input stream with the wrapped stream and the block size
     * @param in
     */
    public BlockInputStream(@Nonnull @WillCloseWhenClosed final InputStream in) {
        super(in);
        Preconditions.checkArgument(in != null, "input stream shouldn't be null");

        count = 0;
        pos = 0;
        hasNext = true;
        closed = false;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();

        if (pos >= count) {
            if (!readBlockSize()) {
                return -1;
            }
        }
        final int b = in.read();
        if (b == -1) {
            throw new EOFException("Invalid block stream, expect 1 byte but reaching out the end of stream");
        }
        pos++;
        return b;
    }

    @Override
    public int read(@Nonnull final byte[] b) throws IOException {
        ensureOpen();
        if (b == null) {
            throw new NullPointerException();
        }
        return read(b, 0, b.length);
    }

    /**
     * Read bytes into b, but at most reading from the inner stream once if necessary
     */
    private int read1(final byte[] b, final int off, final int len) throws IOException {
        int avail = count - pos;
        if (avail <= 0) {
            if (!readBlockSize()) {
                return -1;
            }
            avail = count - pos;
        }

        final int cnt = Math.min(avail, len);
        final int nread = in.read(b, off, cnt);
        if (nread > 0) {
            pos += nread;
        }
        return nread;
    }

    /**
     * This method tries to read as much data as it can under the contract <code>{@link InputStream#read(byte[], int, int) read}</code> method.
     */
    @Override
    public int read(@Nonnull final byte[] b, final int off, final int len) throws IOException {
        ensureOpen();

        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int n = 0;
        while (n < len) {
            final int nread = read1(b, off + n, len - n);
            if (nread <= 0) {
                return (n == 0) ? nread : n;
            }
            n += nread;
        }
        return len;
    }

    @Override
    public long skip(final long n) throws IOException {
        ensureOpen();

        if (n <= 0) {
            return 0;
        }

        int avail = count - pos;
        if (avail <= 0) {
            if (!readBlockSize()) {
                return 0;
            }
            avail = count - pos;
        }

        final long cnt = Math.min(avail, n);
        final long skipped = in.skip(cnt);
        pos += skipped;
        return skipped;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return Math.min(count - pos, in.available());
    }

    @Override
    public synchronized void mark(final int readlimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    /** read the size of next block and return the status if it still has data. */
    private boolean readBlockSize() throws IOException {
        Preconditions.checkState(count >= pos, "There is still some unread data left in the current block");
        if (!hasNext) {
            return false;
        }

        // read the block size
        ByteStreams.readFully(in, blockSizeBytes);
        count = Bytes.bytesToInt(blockSizeBytes);

        if (count < 0) {
            throw new IOException("Invalid block stream, blockSize smaller than 0");
        }

        hasNext = count > 0;
        pos = 0;
        return pos < count;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        in.close();
    }
}
