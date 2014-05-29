package com.indeed.imhotep.io;

import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Class for piping bytes from one thread to another on top of a circular buffer, while minimizing the number of syncs needed
 */
public final class CircularIOStream {
    private static final Logger log = Logger.getLogger(CircularIOStream.class);

    private volatile boolean inputClosed = false;
    private volatile boolean outputClosed = false;

    private final InputStream inputStream;
    private final OutputStream outputStream;

    public CircularIOStream(int bufferSize) throws IOException {
        if (bufferSize <= 0) throw new IllegalArgumentException("bufferSize must be greater than zero");
        final CircularInputStream circularInputStream = new CircularInputStream(bufferSize);
        outputStream = new OutputStream() {

            public void write(final int b) throws IOException {
                circularInputStream.write(b);
            }

            public void write(final byte[] b, final int off, final int len) throws IOException {
                circularInputStream.write(b, off, len);
            }

            public void close() throws IOException {
                outputClosed = true;
                circularInputStream.end();
            }
        };
        inputStream = new FilterInputStream(circularInputStream) {
            public void close() throws IOException {
                inputClosed = true;
                super.close();
            }
        };
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    protected void finalize() throws Throwable {
        if (!inputClosed) {
            log.error("input was not closed, closing in finalizer");
            closeQuietly(inputStream);
        }
        if (!outputClosed) {
            log.error("output was not closed, closing in finalizer");
            closeQuietly(outputStream);
        }
    }

    /**
     * this is very similar to guava's {#link Closeables.closeQuietly()}, except with logging
     * unlike guava this swallows all Exceptions, not just IOExceptions. Error is still propagated.
     * @param closeable closeable to close
     */
    private static void closeQuietly(final Closeable closeable) {
        try {
            if (null != closeable) {
                closeable.close();
            }
        } catch (Exception e) {
            log.error("Exception during cleanup of a Closeable, ignoring", e);
        }
    }

}
