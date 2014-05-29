package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.indeed.imhotep.io.CircularInputStream;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author jplaisance
 */
public final class ActiveBufferedInputStream extends InputStream {
    private static final Logger log = Logger.getLogger(ActiveBufferedInputStream.class);

    final CircularInputStream in;

    public ActiveBufferedInputStream(final InputStream wrapped, int bufferSize) {
        in = new CircularInputStream(bufferSize);
        new Thread(new Runnable() {
            public void run() {
                try {
                    ByteStreams.copy(wrapped, new OutputStream() {
                        public void write(final int b) throws IOException {
                            in.write(b);
                        }

                        public void write(final byte[] b, final int off, final int len) throws IOException {
                            in.write(b, off, len);
                        }

                        public void close() throws IOException {
                            in.end();
                        }
                    });
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }).start();
    }

    public int read() throws IOException {
        return in.read();
    }

    public int read(final byte[] b, final int off, final int len) throws IOException {
        return in.read(b, off, len);
    }

    public void close() throws IOException {
        in.close();
    }

    public int available() throws IOException {
        return in.available();
    }
}
