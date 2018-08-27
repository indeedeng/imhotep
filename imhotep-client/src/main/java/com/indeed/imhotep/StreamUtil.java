package com.indeed.imhotep;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;

public class StreamUtil {

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
