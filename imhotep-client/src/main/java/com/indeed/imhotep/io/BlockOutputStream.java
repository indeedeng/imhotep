package com.indeed.imhotep.io;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author xweng
 *
 * A wrapped output stream used to write data in block to a stream without the knowlege how many bytes
 * it will write in advance, it works with {@link BlockInputStream}.
 *
 * This stream separates the data into several blocks and send them out. The format of each block is:
 * -- 4 bytes --  -- block size bytes --
 *  [block size]      [actual data]
 * At the end of stream, it has an empty block with block size as 0, which indicates no following blocks will come and the stream ends.
 * So except the last empty block, all blocks should have a block size greater than 0.
 *
 * Should close the stream once all data has been written to flush blocks in buffer.
 */
@NotThreadSafe
public class BlockOutputStream extends FilterOutputStream {
    /** The default batch size of stream */
    private static final int DEFAULT_BLOCK_SIZE = 8192;

    private final byte[] buf;
    private final byte[] blockSizeBytes;
    private int count;
    private boolean closed;

    public BlockOutputStream(@Nonnull @WillCloseWhenClosed final OutputStream out) {
        this(out, DEFAULT_BLOCK_SIZE);
    }

    public BlockOutputStream(@Nonnull @WillCloseWhenClosed final OutputStream out, final int blockSize) {
        super(out);

        Preconditions.checkArgument(out != null, "OutputStream shouldn't be null");
        Preconditions.checkArgument(blockSize > 0, "blockSize must be greater than 0");

        buf = new byte[blockSize];
        blockSizeBytes = new byte[4];
        count = 0;
        closed = false;
    }

    @Override
    public void write(final int b) throws IOException {
        ensureOpen();

        if (count >= buf.length) {
            flushBuffer();
        }
        buf[count++] = (byte)b;
    }

    @Override
    public void write(@Nonnull final byte[] b) throws IOException {
        ensureOpen();

        if (b == null) {
            throw new NullPointerException();
        }
        write(b, 0, b.length);
    }

    @Override
    public void write(@Nonnull final byte[] b, final int off, final int len) throws IOException {
        ensureOpen();

        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        if (count + len >= buf.length) {
            writeBlockSize(count + len);
            out.write(buf, 0, count);
            out.write(b, off, len);
            count = 0;
        } else {
            System.arraycopy(b, off, buf, count, len);
            count += len;
        }
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();

        flushBuffer();
        out.flush();
    }

    /**
     * Flush the internal block buffer
     * @throws IOException
     */
    private void flushBuffer() throws IOException {
        // flush only if the buf is not empty
        if (count > 0) {
            writeBlockSize(count);
            out.write(buf, 0, count);
            count = 0;
        }
    }

    /** write the block length and whether last block into stream */
    private void writeBlockSize(final int blockSize) throws IOException {
        Bytes.intToBytes(blockSize, blockSizeBytes);
        out.write(blockSizeBytes);
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

        try {
            flushBuffer();
            // write the empty block indicating stream ends
            writeBlockSize(0);

            out.flush();
        } finally {
            out.close();
        }
    }
}
