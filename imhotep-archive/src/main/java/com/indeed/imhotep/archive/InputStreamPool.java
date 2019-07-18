package com.indeed.imhotep.archive;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Maintains a pool of FSDataInputStreams corresponding to files that are likely to
 * be used multiple times. Not thread-safe.
 *
 * @author dwahler
 */
public class InputStreamPool implements Closeable {
    private static final Logger LOG = Logger.getLogger(InputStreamPool.class);

    private final FileSystem fs;

    // invariant: any StreamWrapper returned by get() is not present in this map until its close() method is called
    private final Map<Path, FSDataInputStream> streams;

    public InputStreamPool(final FileSystem fs) {
        this.fs = fs;
        streams = new HashMap<>();
    }

    public FSDataInputStream get(final Path path) throws IOException {
        final FSDataInputStream innerStream;

        final FSDataInputStream streamFromPool = streams.get(path);
        if (streamFromPool == null) {
            // no available stream, create a new one
            innerStream = fs.open(path);
        } else {
            // we already have a stream open, return it and remove it from the pool
            innerStream = streamFromPool;
            streams.remove(path);
        }

        return new StreamWrapper(path, innerStream);
    }

    public void close() {
        for (Map.Entry<Path, FSDataInputStream> entry : streams.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOG.error("Error closing input stream for file " + entry.getKey(), e);
            }
        }
    }

    private class StreamWrapper extends FSDataInputStream {
        private final Path path;
        private boolean isClosed = false;

        StreamWrapper(final Path path, final FSDataInputStream in) {
            super(in);
            this.path = path;
        }

        @Override
        public void close() throws IOException {
            if (isClosed) {
                return;
            }
            isClosed = true;

            if (!streams.containsKey(path)) {
                // return this stream to the pool
                streams.put(path, (FSDataInputStream) in);
            } else {
                // there's already another stream in the pool for this file, so close this one
                super.close();
            }
        }

        private void ensureNotClosed() {
            Preconditions.checkState(!isClosed, "attempt to reuse stream after it's been returned to pool");
        }

        @Override
        public void seek(final long desired) throws IOException {
            ensureNotClosed();
            super.seek(desired);
        }

        @Override
        public long getPos() throws IOException {
            ensureNotClosed();
            return super.getPos();
        }

        @Override
        public int read(final long position, final byte[] buffer, final int offset, final int length) throws IOException {
            ensureNotClosed();
            return super.read(position, buffer, offset, length);
        }

        @Override
        public void readFully(final long position, final byte[] buffer, final int offset, final int length) throws IOException {
            ensureNotClosed();
            super.readFully(position, buffer, offset, length);
        }

        @Override
        public void readFully(final long position, final byte[] buffer) throws IOException {
            ensureNotClosed();
            super.readFully(position, buffer);
        }

        @Override
        public boolean seekToNewSource(final long targetPos) throws IOException {
            ensureNotClosed();
            return super.seekToNewSource(targetPos);
        }

        @Override
        public InputStream getWrappedStream() {
            ensureNotClosed();
            return super.getWrappedStream();
        }

        @Override
        public int read(final ByteBuffer buf) throws IOException {
            ensureNotClosed();
            return super.read(buf);
        }

        @Override
        public FileDescriptor getFileDescriptor() throws IOException {
            ensureNotClosed();
            return super.getFileDescriptor();
        }

        @Override
        public void setReadahead(final Long readahead) throws IOException, UnsupportedOperationException {
            ensureNotClosed();
            super.setReadahead(readahead);
        }

        @Override
        public void setDropBehind(final Boolean dropBehind) throws IOException, UnsupportedOperationException {
            ensureNotClosed();
            super.setDropBehind(dropBehind);
        }

        @Override
        public ByteBuffer read(final ByteBufferPool bufferPool, final int maxLength, final EnumSet<ReadOption> opts) throws IOException, UnsupportedOperationException {
            ensureNotClosed();
            return super.read(bufferPool, maxLength, opts);
        }

        @Override
        public void releaseBuffer(final ByteBuffer buffer) {
            ensureNotClosed();
            super.releaseBuffer(buffer);
        }

        @Override
        public void unbuffer() {
            ensureNotClosed();
            super.unbuffer();
        }

        @Override
        public int read() throws IOException {
            ensureNotClosed();
            return super.read();
        }

        @Override
        public long skip(final long n) throws IOException {
            ensureNotClosed();
            return super.skip(n);
        }

        @Override
        public int available() throws IOException {
            ensureNotClosed();
            return super.available();
        }

        @Override
        public synchronized void mark(final int readlimit) {
            ensureNotClosed();
            super.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            ensureNotClosed();
            super.reset();
        }

        @Override
        public boolean markSupported() {
            ensureNotClosed();
            return super.markSupported();
        }
    }
}
