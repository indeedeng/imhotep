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
 * The class allows at most a 0-data block, and it must be the last block if it exists.
 * It will be considerd stream has been read fully when reading an empty block.
 *
 * The inner input stream won't be closed when {@link BlockInputStream} is closed. You need to close the inner stream manually if necessary.
 * Also you need to read through the end of the block input stream, or otherwise the original input stream will contain garbage.
 */
@NotThreadSafe
public class BlockInputStream extends FilterInputStream {
    private int count;
    private int pos;

    private boolean lastBlock;
    private boolean closed;

    private final byte[] blockSizeBytes = new byte[4];

    /**
     * Initialize a block input stream with the wrapped stream and the block size
     * @param in
     */
    public BlockInputStream(@Nonnull @WillNotClose final InputStream in) {
        super(in);
        Preconditions.checkArgument(in != null, "input stream shouldn't be null");

        count = 0;
        pos = 0;
        lastBlock = false;
        closed = false;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();

        if (pos >= count) {
            if (!readBlockHeader()) {
                return -1;
            }
        }
        final int b = in.read();
        if (b != -1) {
            pos++;
        }
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
            if (!readBlockHeader()) {
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
     * This method tries to read as more data as it can under the contract <code>{@link InputStream#read(byte[], int, int) read}</code> method.
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
            if (!readBlockHeader()) {
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

    /** read the header of the next block and return the status if it still has data. */
    private boolean readBlockHeader() throws IOException {
        Preconditions.checkState(count >= pos, "There is still some unread data left in the current block");
        if (lastBlock) {
            return false;
        }

        // read the block size
        final int n = readFully(in, blockSizeBytes, 0, blockSizeBytes.length);
        if (n != blockSizeBytes.length) {
            throw new IOException("Invalid block stream, read " + n  + ", expect " + blockSizeBytes.length);
        }
        count = Bytes.bytesToInt(blockSizeBytes);

        // read the last block byte
        final int lastBlockByte = in.read();
        if (lastBlockByte == -1) {
            throw new IOException("Invalid block stream, no byte is available");
        } else if (lastBlockByte != 0 && lastBlockByte != 1) {
            throw new IOException("Invalid block stream, lastBlockByte should be set as 0 or 1");
        }
        lastBlock = lastBlockByte == 1;

        if (count < 0 || (count == 0 && !lastBlock)) {
            throw new IOException("Invalid block stream, blockSize smaller than 0 or equal to 0 but not the last block");
        }
        pos = 0;
        return pos < count;
    }

    private static int readFully(final InputStream is, final byte[] b, final int off, final int len) throws IOException {
        int n = 0;
        while (n < len) {
            int nread = is.read(b, off + n, len - n);
            if (nread < 0) {
                break;
            }
            n += nread;
        }
        return n;
    }

    private void ensureOpen() throws IOException {
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
