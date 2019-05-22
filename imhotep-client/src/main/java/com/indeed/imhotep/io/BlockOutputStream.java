package com.indeed.imhotep.io;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.WillNotClose;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author xweng
 *
 * A wrapped output stream used to write data in block to a keep-open stream without the knowlege how many bytes
 * it will write in advance, it should work with {@link BlockInputStream}.
 *
 * This class separates the data into several blocks and send them out. The format of each block is:
 * -- 4 bytes --  ---- 1 byte ----  -- block length bytes --
 * [block length] [last block flag] [data]
 *
 * The class allows at most a 0-data block, and it must be the last block if it exists. In general, you should ensure all blocks have data.
 *
 * Should close the stream once all data has been written to flush blocks in buffer. The close method won't close the inner
 * stream, you should close the inner stream manually if necessary.
 */
@NotThreadSafe
public class BlockOutputStream extends FilterOutputStream {
    /** The default batch size of stream */
    private static final int DEFAULT_BLOCK_SIZE = 8192;

    private final byte[] buf;
    private int count;
    private boolean closed;

    public BlockOutputStream(@Nonnull @WillNotClose final OutputStream out) {
        this(out, DEFAULT_BLOCK_SIZE);
    }

    public BlockOutputStream(@Nonnull @WillNotClose final OutputStream out, final int blockSize) {
        super(out);

        Preconditions.checkArgument(out != null, "OutputStream shouldn't be null value");
        Preconditions.checkArgument(blockSize > 0, "batchSize must be greater than 0");

        buf = new byte[blockSize];
        count = 0;
        closed = false;
    }

    @Override
    public void write(final int b) throws IOException {
        ensureOpen();

        if (count >= buf.length) {
            flushBuffer(false);
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

        int nwrite = 0;
        while (nwrite < len) {
            if (count >= buf.length) {
                flushBuffer(false);
            }

            final int cnt;
            // If the buffer is empty and we have sufficient data to write, then writing it directly to avoid local copy
            if (count == 0 && len - nwrite >= buf.length) {
                cnt = len - nwrite;
                writeBlockHeader(cnt, false);
                out.write(b, off + nwrite, cnt);
            } else {
                cnt = Math.min(len - nwrite, buf.length - count);
                System.arraycopy(b, off + nwrite, buf, count, cnt);
                count += cnt;
            }
            nwrite += cnt;
        }
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();

        flushBuffer(false);
        out.flush();
    }

    /**
     * Flush the internal block buffer
     * @param lastBlock Whether the data in the buffer is the last block. If so, it will set the lastBlock byte as 1 and send.
     * @throws IOException
     */
    private void flushBuffer(final boolean lastBlock) throws IOException {
        // if there is data in buf, flush them anyway
        if (count > 0) {
            writeBlockHeader(count, lastBlock);
            out.write(buf, 0, count);
            count = 0;
        // in case the byte last-block need to be sent even there is no data in buf.
        // flushBuffer(true) will be called once and only once in the close method
        } else if (lastBlock) {
            writeBlockHeader(0, true);
        }
    }

    /** write the block length and whether last block into stream */
    private void writeBlockHeader(final int blockSize, final boolean lastBlock) throws IOException {
        out.write(Bytes.intToBytes(blockSize));
        out.write(lastBlock ? 1 : 0);
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
        flushBuffer(true);
        out.flush();
        closed = true;
    }
}
