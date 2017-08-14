package com.indeed.imhotep.io;

import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author jplaisance
 */
public final class MultiFile {
    private static final Logger log = Logger.getLogger(MultiFile.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private SharedReference<MMapBuffer> buffer;
    private final File path;
    private final int blockSize;
    private final Split[] splits;
    @Nullable
    private final AtomicLong bytesAvailable;

    public static MultiFile create(final File path, final int splits) throws IOException {
        return create(path, splits, 1024 * 1024);
    }

    public static MultiFile create(final File path, final int splits, final int blockSize) throws IOException {
        return new MultiFile(path, splits, blockSize, null);
    }

    public static MultiFile create(final File path, final int splits, final int blockSize, @Nullable final AtomicLong bytesAvailable) throws IOException {
        return new MultiFile(path, splits, blockSize, bytesAvailable);
    }

    private MultiFile(final File path, final int splits, final int blockSize, @Nullable final AtomicLong bytesAvailable) throws IOException {
        if (blockSize <= 0 || blockSize % 4096 != 0) {
            throw new IllegalArgumentException("block size cannot be "+blockSize+", it must be a positive multiple of 4096");
        }
        if (splits <= 0) {
            throw new IllegalArgumentException("splits cannot be "+splits+", it must be > 0");
        }
        this.bytesAvailable = bytesAvailable;
        this.path = path;
        this.blockSize = blockSize;
        final Closer closer = Closer.create();
        try {
            final RandomAccessFile raf = new RandomAccessFile(path, "rw");
            final SharedReference<RandomAccessFile> rafRef = closer.register(SharedReference.create(raf));
            buffer = closer.register(SharedReference.create(new MMapBuffer(raf, path, 0, blockSize * splits, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN)));
            this.splits = new Split[splits];
            final AtomicInteger openInputs = new AtomicInteger(splits);
            for (int i = 0; i < splits; i++) {
                this.splits[i] = new Split(i, rafRef, openInputs);
            }
            rafRef.close();
        } catch (final Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables2.propagate(t, IOException.class);
        }
    }

    public OutputStream getOutputStream(final int split) {
        return splits[split].out;
    }

    public InputStream getInputStream(final int split) {
        return splits[split].in;
    }

    private final class Split {
        private final OutputStream out;
        private final InputStream in;
        private long writePosition;

        private Split(final int split, final SharedReference<RandomAccessFile> raf, final AtomicInteger openInputs) {
            out = new MultiFileOutputStream(split, raf);
            in = new MultiFileInputStream(split, openInputs);
        }

        private final class MultiFileOutputStream extends OutputStream {
            private final SharedReference<RandomAccessFile> raf;
            private SharedReference<MMapBuffer> currentBuffer;
            private DirectMemory memory;
            private final int split;
            long block = 0;
            long limit;
            Lock currentLock;

            private MultiFileOutputStream(final int split, final SharedReference<RandomAccessFile> raf) {
                this.split = split;
                writePosition = blockSize*split;
                this.limit = writePosition+blockSize;
                currentBuffer = buffer.copy();
                memory = currentBuffer.get().memory();
                this.raf = raf.copy();
            }

            @Override
            public void write(final int b) throws IOException {
                if (writePosition == limit) {
                    seekToNextBlock();
                }
                memory.putByte(writePosition, (byte) b);
                writePosition++;
            }

            @Override
            public void write(@Nonnull final byte[] b, int off, int len) throws IOException {
                if (off < 0) {
                    throw new IllegalArgumentException("off is "+off);
                }
                if (len < 0) {
                    throw new IllegalArgumentException("len is "+len);
                }
                if (off+len > b.length) {
                    throw new IllegalArgumentException("off is "+off+", len is "+len+", b.length is "+b.length);
                }
                while (len > 0) {
                    if (writePosition == limit) {
                        seekToNextBlock();
                    }
                    final int bytesToCopy = (int) Math.min(len, limit-writePosition);
                    memory.putBytes(writePosition, b, off, bytesToCopy);
                    off += bytesToCopy;
                    len -= bytesToCopy;
                    writePosition += bytesToCopy;
                }
            }

            private void seekToNextBlock() throws IOException {
                if (bytesAvailable != null && bytesAvailable.addAndGet(-blockSize) < 0) {
                    throw new WriteLimitExceededException();
                }
                block++;
                writePosition = blockSize*(block*splits.length+split);
                limit = writePosition+blockSize;
                if (limit > memory.length()) {
                    currentLock = lock.readLock();
                    currentLock.lock();
                    try {
                        internalSeekToNextBlock(false);
                    } finally {
                        currentLock.unlock();
                        currentLock = null;
                    }
                }
            }

            private void internalSeekToNextBlock(final boolean writeLocked) throws IOException {
                final SharedReference<MMapBuffer> ref = buffer.copy();
                try {
                    if (limit <= ref.get().memory().length()) {
                        Closeables2.closeQuietly(currentBuffer, log);
                        currentBuffer = ref;
                        memory = currentBuffer.get().memory();
                        return;
                    }
                } catch (final Throwable t) {
                    Closeables2.closeQuietly(ref, log);
                    throw Throwables.propagate(t);
                }
                ref.close();
                if (writeLocked) {
                    final RandomAccessFile file = raf.get();
                    file.setLength(file.length() * 2);
                    final SharedReference<MMapBuffer> old = buffer;
                    buffer = SharedReference.create(new MMapBuffer(file, path, 0, file.length(), FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN));
                    Closeables2.closeAll(log, old, currentBuffer);
                    currentBuffer = buffer.copy();
                    memory = currentBuffer.get().memory();
                } else {
                    lock.readLock().unlock();
                    currentLock = lock.writeLock();
                    currentLock.lock();
                    internalSeekToNextBlock(true);
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    currentBuffer.close();
                } finally {
                    raf.close();
                }
            }
        }

        private final class MultiFileInputStream extends InputStream {

            private final int split;
            private final AtomicInteger openInputs;
            private long position = 0;
            private long limit = 0;
            private long block = -1;
            private SharedReference<MMapBuffer> currentBuffer = null;
            private DirectMemory memory = null;

            private MultiFileInputStream(final int split, final AtomicInteger openInputs) {
                this.split = split;
                this.openInputs = openInputs;
            }

            @Override
            public int read() throws IOException {
                if (position >= writePosition) {
                    return -1;
                }
                if (position == limit) {
                    seekToNextBlock();
                    //i don't think this check is necessary but it doesn't hurt that much
                    if (position >= writePosition) {
                        return -1;
                    }
                }
                final int ret = memory.getByte(position) & 0xFF;
                position++;
                return ret;
            }

            private void seekToNextBlock() throws IOException {
                final long newLimit = blockSize*((block+1)*splits.length+split+1);
                if (memory == null || newLimit > memory.length()) {
                    lock.readLock().lock();
                    try {
                        Closeables2.closeQuietly(currentBuffer, log);
                        currentBuffer = buffer.copy();
                        memory = currentBuffer.get().memory();
                        if (newLimit > memory.length()) {
                            throw new IllegalStateException();
                        }
                    } finally {
                        lock.readLock().unlock();
                    }
                }
                block++;
                position = newLimit-blockSize;
                limit = newLimit;
            }

            @Override
            public int read(@Nonnull final byte[] b, int off, int len) throws IOException {
                if (off < 0) {
                    throw new IllegalArgumentException("off is "+off);
                }
                if (len < 0) {
                    throw new IllegalArgumentException("len is "+len);
                }
                if (off+len > b.length) {
                    throw new IllegalArgumentException("off is "+off+", len is "+len+", b.length is "+b.length);
                }
                if (len == 0) {
                    return 0;
                }
                int ret = 0;
                while (len > 0 && position < writePosition) {
                    if (position == limit) {
                        seekToNextBlock();
                        continue;
                    }
                    final int bytesToRead = (int) Math.min(len, Math.min(writePosition, limit)-position);
                    memory.getBytes(position, b, off, bytesToRead);
                    position += bytesToRead;
                    ret += bytesToRead;
                    off += bytesToRead;
                    len -= bytesToRead;
                }
                return (ret > 0) ? ret : -1;
            }

            @Override
            public void close() throws IOException {
                try {
                    if (currentBuffer != null) {
                        currentBuffer.close();
                    }
                } finally {
                    if (openInputs.decrementAndGet() == 0) {
                        lock.writeLock().lock();
                        try {
                            buffer.close();
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                }
            }
        }
    }
}
