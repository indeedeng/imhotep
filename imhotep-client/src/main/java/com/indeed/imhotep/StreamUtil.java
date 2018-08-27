package com.indeed.imhotep;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamUtil {

    public static class InputStreamWithPosition extends InputStream {
        private long position;
        private final InputStream in;

        public InputStreamWithPosition(final InputStream in) {
            this.in = in;
        }

        public long getPosition() {
            return position;
        }

        // seek forward to position or fail
        public void seekForward(final long newPosition) throws IOException {
            if (position > newPosition) {
                throw new IllegalStateException("Can't seek backward");
            }

            while (position < newPosition) {
                skip(newPosition - position);
            }

            if (position != newPosition) {
                throw new IllegalStateException("Seek failed. Current position is " + position + ", expected position is " + newPosition);
            }
        }

        @Override
        public int read() throws IOException {
            final int result = in.read();
            position++;
            return result;
        }

        @Override
        public int read(@Nonnull final byte[] b) throws IOException {
            final int result = in.read(b);
            if (result > 0) {
                position += result;
            }
            return result;
        }

        @Override
        public int read(@Nonnull final byte[] b, final int off, final int len) throws IOException {
            final int result = in.read(b, off, len);
            if (result > 0) {
                position += result;
            }
            return result;
        }

        @Override
        public long skip(final long n) throws IOException {
            final long result = in.skip(n);
            position += result;
            return result;
        }

        @Override
        public int available() throws IOException {
            return in.available();
        }

        @Override
        public void close() throws IOException {
            in.close();
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
    }

    public static class OutputStreamWithPosition extends OutputStream {
        private long position;
        private final OutputStream out;

        public OutputStreamWithPosition(final OutputStream out) {
            this.out = out;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public void write(final int b) throws IOException {
            out.write(b);
            position++;
        }

        @Override
        public void write(@Nonnull final byte[] b) throws IOException {
            out.write(b);
            position += b.length;
        }

        @Override
        public void write(@Nonnull final byte[] b, final int off, final int len) throws IOException {
            out.write(b, off, len);
            position += len;
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

}
