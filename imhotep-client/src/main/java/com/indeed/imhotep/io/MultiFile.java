package com.indeed.imhotep.io;

import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author jplaisance
 */
public final class MultiFile {
    private static final Logger log = Logger.getLogger(MultiFile.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final RandomAccessFile raf;
    private SharedReference<MMapBuffer> buffer;
    private final File path;
    private final int blockSize;
    private final Split[] splits;

    public static MultiFile create(File path, int splits) throws IOException {
        return create(path, splits, 1024 * 1024);
    }

    public static MultiFile create(File path, int splits, int blockSize) throws IOException {
        return new MultiFile(path, splits, blockSize);
    }

    private MultiFile(File path, int splits, int blockSize) throws IOException {
        if (blockSize <= 0 || blockSize % 4096 != 0) {
            throw new IllegalArgumentException("block size is "+blockSize+", it must be a positive multiple of 4096");
        }
        this.path = path;
        this.blockSize = blockSize;
        final Closer closer = Closer.create();
        try {
            raf = closer.register(new RandomAccessFile(path, "rw"));
            buffer = closer.register(SharedReference.create(new MMapBuffer(raf, path, 0, blockSize * splits, FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN)));
            this.splits = new Split[splits];
            for (int i = 0; i < splits; i++) {
                this.splits[i] = new Split(i);
            }
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables2.propagate(t, IOException.class);
        }
    }

    OutputStream getOutputStream(int split) {
        return splits[split].out;
    }

    InputStream getInputStream(int split) {
        return splits[split].in;
    }

    private final class Split {
        private final OutputStream out;
        private final InputStream in;
        private long writePosition;

        private Split(int split) {
            out = new MultiFileOutputStream(split);
            in = new MultiFileInputStream(split);
        }

        private final class MultiFileOutputStream extends OutputStream {

            private SharedReference<MMapBuffer> currentBuffer;
            private DirectMemory memory;
            private final int split;
            long block = 0;
            long limit;
            Lock currentLock;

            private MultiFileOutputStream(int split) {
                this.split = split;
                writePosition = blockSize*split;
                this.limit = writePosition+blockSize;
                currentBuffer = buffer.copy();
                memory = currentBuffer.get().memory();
            }

            @Override
            public void write(int b) throws IOException {
                if (writePosition == limit) {
                    seekToNextBlock();
                }
                memory.putByte(writePosition, (byte) b);
                writePosition++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
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
                block++;
                writePosition = blockSize*(block*splits.length+split);
                limit = writePosition+blockSize;
                if (limit > memory.length()) {
                    currentLock = lock.readLock();
                    lock.readLock().lock();
                    try {
                        internalSeekToNextBlock(false);
                    } finally {
                        currentLock.unlock();
                        currentLock = null;
                    }
                }
            }

            private void internalSeekToNextBlock(boolean writeLocked) throws IOException {
                final SharedReference<MMapBuffer> ref = buffer.copy();
                try {
                    if (limit <= ref.get().memory().length()) {
                        currentBuffer = ref;
                        memory = currentBuffer.get().memory();
                        return;
                    }
                } catch (Throwable t) {
                    Closeables2.closeQuietly(ref, log);
                    throw Throwables.propagate(t);
                }
                ref.close();
                if (writeLocked) {
                    raf.setLength(raf.length()*2);
                    final SharedReference<MMapBuffer> old = buffer;
                    buffer = SharedReference.create(new MMapBuffer(raf, path, 0, raf.length(), FileChannel.MapMode.READ_WRITE, ByteOrder.LITTLE_ENDIAN));
                    old.close();
                } else {
                    lock.readLock().unlock();
                    currentLock = lock.writeLock();
                    lock.writeLock().lock();
                    internalSeekToNextBlock(true);
                }
            }

            @Override
            public void close() throws IOException {
                currentBuffer.close();
            }
        }

        private final class MultiFileInputStream extends InputStream {

            private final int split;
            private long position = 0;
            private long limit = 0;
            private long block = -1;
            private SharedReference<MMapBuffer> currentBuffer = null;
            private DirectMemory memory = null;

            public MultiFileInputStream(int split) {
                this.split = split;
            }

            @Override
            public int read() throws IOException {
                if (position >= writePosition) return -1;
                if (position == limit) {
                    seekToNextBlock();
                    //i don't think this check is necessary but it doesn't hurt that much
                    if (position >= writePosition) return -1;
                }
                return memory.getByte(position)&0xFF;
            }

            private void seekToNextBlock() throws IOException {
                final long newLimit = blockSize*((block+1)*splits.length+split+1);
                if (memory == null || newLimit > memory.length()) {
                    lock.readLock().lock();
                    try {
                        currentBuffer.close();
                        currentBuffer = buffer.copy();
                        memory = currentBuffer.get().memory();
                        if (newLimit > memory.length()) throw new IllegalStateException();
                    } finally {
                        lock.readLock().unlock();
                    }
                }
                block++;
                position = newLimit-blockSize;
                limit = newLimit;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (off < 0) {
                    throw new IllegalArgumentException("off is "+off);
                }
                if (len < 0) {
                    throw new IllegalArgumentException("len is "+len);
                }
                if (off+len > b.length) {
                    throw new IllegalArgumentException("off is "+off+", len is "+len+", b.length is "+b.length);
                }
                if (len == 0) return 0;
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
                if (ret > 0 ) return ret;
                return -1;
            }

            @Override
            public void close() throws IOException {
                super.close();
            }
        }
    }
}
