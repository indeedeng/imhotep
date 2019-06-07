package com.indeed.imhotep.connection;

import javax.annotation.WillNotClose;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author xweng
 */
public class NonClosingInputStream extends InputStream {
    private boolean closed;
    private final InputStream in;

    NonClosingInputStream(@WillNotClose final InputStream in) {
        this.in = in;
        this.closed = false;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        return in.read();
    }

    @Override
    public int read(final byte[] b) throws IOException {
        ensureOpen();
        return in.read(b);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        ensureOpen();
        return in.read(b, off, len);
    }

    @Override
    public long skip(final long n) throws IOException {
        ensureOpen();
        return in.skip(n);
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return in.available();
    }

    @Override
    public synchronized void mark(final int readlimit) {
        if (closed) {
            return;
        }
        in.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        ensureOpen();
        in.reset();
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("The stream has been closed, no longer allowed to read from the underlying stream");
        }
    }
}
