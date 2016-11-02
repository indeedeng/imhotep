package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * FileLock for multi-process multi-thread
 * - tryReadLock isn't implemented yet.
 * - ordering policy is a bit weird;
 *   even if there is another process that read-locking a file,
 *   read-lock might be blocked by a thread in the same process which is waiting for write-lock
 * @author michihiko
 */

abstract class MultiThreadFileLock implements Closeable {

    public abstract boolean isClosed();

    @Nonnull
    public abstract FileChannel getFileChannel();

    private static class ReadWriteLockWithFileLockImpl {
        private final File file;
        // Thread-local read-write lock
        private final ReentrantReadWriteLock threadLock;
        // absent iff there are no thread that acquires the resource currently.
        private Optional<FileLock> fileLock;
        // Thread-local lock that locked when we're going to lock the file.
        private final ReentrantLock fileAccessLock;

        private ReadWriteLockWithFileLockImpl(@Nonnull final File file) {
            this(file, true);
        }

        private ReadWriteLockWithFileLockImpl(@Nonnull final File file, final boolean fair) {
            this.file = file;
            this.threadLock = new ReentrantReadWriteLock(fair);
            this.fileLock = Optional.absent();
            this.fileAccessLock = new ReentrantLock(fair);
        }

        /**
         * Lock {@code lock}, and acquires file lock if this is the first lock in the process.
         */
        private FileChannel acquireFileLock(@Nonnull final Lock lock) throws IOException {
            //noinspection LockAcquiredButNotSafelyReleased
            lock.lock();
            synchronized (this) {
                // This block takes a time =>  !fileLock.isPresent() <=> all locks had already been closed.
                // So, this doesn't blocks unlock() method long time.
                Preconditions.checkState(threadLock.isWriteLockedByCurrentThread() || (threadLock.getReadHoldCount() > 0));
                fileAccessLock.lock();
                try {
                    if (threadLock.isWriteLocked()) {
                        Preconditions.checkState(!fileLock.isPresent());
                        fileLock = Optional.of(FileLockUtil.lock(false, file));
                        return fileLock.get().channel();
                    } else {
                        if (fileLock.isPresent()) {
                            return fileLock.get().channel();
                        }
                        fileLock = Optional.of(FileLockUtil.lock(true, file));
                        return fileLock.get().channel();
                    }
                } catch (final IOException e) {
                    lock.unlock();
                    throw e;
                } finally {
                    fileAccessLock.unlock();
                }
            }
        }

        /**
         * Unlock {@code lock}, and release file lock if this is the last lock in the process.
         */
        private synchronized void unlock(@Nonnull final Lock lock) throws IOException {
            fileAccessLock.lock();
            try {
                if (threadLock.isWriteLockedByCurrentThread() || (threadLock.getReadLockCount() == 1)) {
                    Preconditions.checkState(fileLock.isPresent());
                    //noinspection OptionalGetWithoutIsPresent
                    fileLock.get().close();
                }
            } catch (final IOException e) {
                fileLock = Optional.absent();
                throw e;
            } finally {
                fileAccessLock.unlock();
                lock.unlock();
            }
        }

        private class LockObject extends MultiThreadFileLock {
            private final FileChannel fileChannel;
            private final Lock lock;
            private final AtomicBoolean closed;

            private LockObject(@Nonnull final FileChannel fileChannel, @Nonnull final Lock lock) {
                this.fileChannel = fileChannel;
                this.closed = new AtomicBoolean(false);
                this.lock = lock;
            }

            @Override
            public void close() throws IOException {
                if (!closed.getAndSet(true)) {
                    unlock(lock);
                }
            }

            @Override
            public boolean isClosed() {
                return closed.get();
            }

            @Nonnull
            @Override
            public FileChannel getFileChannel() {
                return fileChannel;
            }
        }

        @Nonnull
        final MultiThreadFileLock readLock() throws IOException {
            final Lock lock = threadLock.readLock();
            return new LockObject(acquireFileLock(lock), lock);
        }

        @Nonnull
        final MultiThreadFileLock writeLock() throws IOException {
            final Lock lock = threadLock.writeLock();
            return new LockObject(acquireFileLock(lock), lock);
        }

        @Nonnull
        final Optional<MultiThreadFileLock> tryWriteLock() throws IOException {
            final Lock lock = threadLock.writeLock();
            if (!lock.tryLock()) {
                return Optional.absent();
            }
            Preconditions.checkState(!fileLock.isPresent());
            Preconditions.checkState(fileAccessLock.tryLock());
            try {
                fileLock = FileLockUtil.tryLock(false, file);
            } catch (final IOException e) {
                fileAccessLock.unlock();
                lock.unlock();
                throw e;
            }
            if (fileLock.isPresent()) {
                final FileChannel channel = fileLock.get().channel();
                fileAccessLock.unlock();
                return Optional.of((MultiThreadFileLock) new LockObject(channel, lock));
            } else {
                fileAccessLock.unlock();
                lock.unlock();
                return Optional.absent();
            }
        }
    }

    private static final Map<String, ReadWriteLockWithFileLockImpl> LOCKS = new HashMap<>();

    @Nonnull
    private static ReadWriteLockWithFileLockImpl getReadWriteLockImpl(@Nonnull final Path path) throws IOException {
        final File file = path.toFile();
        //noinspection ResultOfMethodCallIgnored
        file.createNewFile();
        final String canonicalPath = file.getCanonicalPath();
        synchronized (LOCKS) {
            if (!LOCKS.containsKey(canonicalPath)) {
                LOCKS.put(canonicalPath, new ReadWriteLockWithFileLockImpl(file));
            }
            final ReadWriteLockWithFileLockImpl lock = LOCKS.get(canonicalPath);
            return Preconditions.checkNotNull(lock);
        }
    }

    @Nonnull
    static MultiThreadFileLock lock(final boolean shared, @Nonnull final Path path, @Nonnull final String... file) throws IOException {
        if (shared) {
            return readLock(path, file);
        } else {
            return writeLock(path, file);
        }
    }

    @Nonnull
    static MultiThreadFileLock readLock(@Nonnull final Path path, @Nonnull final String... file) throws IOException {
        return getReadWriteLockImpl(Paths.get(path.toString(), file)).readLock();
    }

    @Nonnull
    static MultiThreadFileLock writeLock(@Nonnull final Path path, @Nonnull final String... file) throws IOException {
        return getReadWriteLockImpl(Paths.get(path.toString(), file)).writeLock();

    }

    @Nonnull
    static Optional<MultiThreadFileLock> tryWriteLock(@Nonnull final Path path, @Nonnull final String... file) throws IOException {
        return getReadWriteLockImpl(Paths.get(path.toString(), file)).tryWriteLock();

    }
}
