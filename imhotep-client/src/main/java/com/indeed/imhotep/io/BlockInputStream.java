package com.indeed.imhotep.io;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.WillNotClose;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author xweng
 *
 * A wrapped input stream to read data written by {@link BlockOutputStream}. It continuously reads data until the last-block
 * byte is 1.
 *
 * This class read data by blocks. The format of each block is:
 * -- 4 bytes --  ---- 1 byte ----  -- block length bytes --
 * [block length] [last block flag] [data]
 *
 * The inner input stream won't be closed when {@link BlockInputStream} is closed. You need to close the inner stream manually if necessary.
 * Also you need to read through the end of the block input stream, or otherwise the original input stream will contain garbage.
 */
@NotThreadSafe
public class BlockInputStream extends FilterInputStream {
    /** The default block size of stream */
    private static final int DEFAULT_BLOCK_SIZE = 8192;

    private int count;
    private int pos;

    private boolean lastBlock;
    private boolean closed;

    private final byte[] blockSizeBytes = new byte[4];

    public BlockInputStream(@Nonnull @WillNotClose final InputStream in) {
        this(in, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Initialize a block input stream with the wrapped stream and the block size
     * @param in
     * @param blockSize
     */
    public BlockInputStream(@Nonnull @WillNotClose final InputStream in, final int blockSize) {
        super(in);

        Preconditions.checkArgument(in != null, "input stream shouldn't be null");
        Preconditions.checkArgument(blockSize > 0, "blockSize should be greater than 0");

        count = 0;
        pos = 0;
        lastBlock = false;
        closed = false;
    }

    @Override
    public int read() throws IOException {
        closeCheck();

        if (pos >= count) {
            fill();
            if (pos >= count) {
                return -1;
            }
        }
        pos++;
        return in.read();
    }

    @Override
    public int read(@Nonnull final byte[] b) throws IOException {
        closeCheck();
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
            fill();
            avail = count - pos;
            if (avail <= 0) {
                return -1;
            }
        }

        final int cnt = (avail < len) ? avail : len;
        final int nread = in.read(b, off, cnt);
        if (nread != cnt) {
            throw new IOException("Invalid block stream, read " + nread + ", expect " + cnt);
        }
        pos += cnt;
        return cnt;
    }

    @Override
    public int read(@Nonnull final byte[] b, final int off, final int len) throws IOException {
        closeCheck();

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
        closeCheck();

        if (n <= 0) {
            return 0;
        }
        long skipped = 0;
        while (skipped < n) {
            int avail = count - pos;
            if (avail <= 0) {
                fill();
                avail = count - pos;
                if (avail <= 0) {
                    break;
                }
            }

            final long toSkip = Math.min(avail, n - skipped);
            in.skip(toSkip);
            pos += toSkip;
            skipped += toSkip;
        }
        return skipped;
    }

    @Override
    public int available() throws IOException {
        closeCheck();
        return count - pos;
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

    private void fill() throws IOException {
        if (lastBlock) {
            return;
        }

        // read the block size
        final int n = in.read(blockSizeBytes);
        if (n != blockSizeBytes.length) {
            throw new IOException("Invalid block stream, read " + n  + ", expect " + blockSizeBytes.length);
        }
        count = Bytes.bytesToInt(blockSizeBytes);

        // read the last block byte
        final int lastBlockByte = in.read();
        if (lastBlockByte == -1) {
            throw new IOException("Invalid block stream, no byte is available");
        }
        lastBlock = lastBlockByte == 1;
        pos = 0;
    }

    private void closeCheck() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    /**
     * Won't close the inner input stream
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
    }
}
