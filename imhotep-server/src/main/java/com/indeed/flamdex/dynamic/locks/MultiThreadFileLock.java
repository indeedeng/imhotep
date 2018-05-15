/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.flamdex.dynamic.locks;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

/**
 * FileLock for multi-process multi-thread.
 * Please use from {@link MultiThreadFileLockUtil}.
 * - ordering policy is a bit weird;
 * even if there is another process that read-locking a file,
 * read-lock might be blocked by a thread in the same process which is waiting for write-lock
 *
 * @author michihiko
 */
class MultiThreadFileLock {
    private final Path path;
    // Process-local read-write lock
    private final StampedLock processLocalLock;
    // absent iff there are no thread that acquires the resource currently.
    private Optional<FileChannel> fileLock;
    // Process-local exclusive lock to make sure there is only one thread going to acquire/release fileLock at the same moment.
    private final ReentrantLock fileAccessLock;

    MultiThreadFileLock(@Nonnull final Path path) {
        this(path, true);
    }

    MultiThreadFileLock(@Nonnull final Path path, final boolean fair) {
        this.path = path;
        this.processLocalLock = new StampedLock();
        this.fileLock = Optional.absent();
        this.fileAccessLock = new ReentrantLock(fair);
    }

    /**
     * Lock {@code lock}, and acquires file lock if this is the first lock in the process.
     */
    private void acquireFileLock(@Nonnull final Lock lock) throws IOException {
        //noinspection LockAcquiredButNotSafelyReleased
        lock.lock();
        try {
            fileAccessLock.lock();
            try {
                // This block takes a time =>  !fileLock.isPresent() <=> all locks had already been closed.
                // So, this doesn't blocks unlock() method long time.
                Preconditions.checkState(processLocalLock.isReadLocked() || processLocalLock.isWriteLocked());
                if (processLocalLock.isWriteLocked()) {
                    Preconditions.checkState(!fileLock.isPresent());
                    fileLock = Optional.of(FileLockUtil.writeLock(path));
                } else if (!fileLock.isPresent()) {
                    fileLock = Optional.of(FileLockUtil.readLock(path));
                }
            } finally {
                fileAccessLock.unlock();
            }
        } catch (final Throwable e) {
            lock.unlock();
            throw e;
        }
    }

    /**
     * Unlock {@code lock}, and release file lock if this is the last lock in the process.
     */
    private void unlock(@Nonnull final Lock lock) throws IOException {
        fileAccessLock.lock();
        try {
            if (processLocalLock.isWriteLocked() || (processLocalLock.getReadLockCount() == 1)) {
                Preconditions.checkState(fileLock.isPresent());
                //noinspection OptionalGetWithoutIsPresent
                fileLock.get().close();
                fileLock = Optional.absent();
            }
        } finally {
            try {
                lock.unlock();
            } finally {
                fileAccessLock.unlock();
            }
        }
    }

    private class MultiThreadLockImpl implements MultiThreadLock {
        private final boolean shared;
        private final Lock lock;
        private final AtomicBoolean closed;

        private MultiThreadLockImpl(final boolean shared, @Nonnull final Lock lock) {
            this.shared = shared;
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

        @Override
        public boolean isShared() {
            return shared;
        }
    }

    @Nonnull
    MultiThreadLock readLock() throws IOException {
        final Lock lock = processLocalLock.asReadLock();
        acquireFileLock(lock);
        return new MultiThreadLockImpl(true, lock);
    }

    @Nonnull
    MultiThreadLock writeLock() throws IOException {
        final Lock lock = processLocalLock.asWriteLock();
        acquireFileLock(lock);
        return new MultiThreadLockImpl(false, lock);
    }

    @Nonnull
    Optional<MultiThreadLock> tryReadLock() throws IOException {
        final Lock lock = processLocalLock.asReadLock();
        if (!lock.tryLock()) {
            return Optional.absent();
        }
        try {
            if (fileAccessLock.tryLock()) {
                try {
                    if (!fileLock.isPresent()) {
                        fileLock = FileLockUtil.tryReadLock(path);
                    }
                    if (fileLock.isPresent()) {
                        return Optional.<MultiThreadLock>of(new MultiThreadLockImpl(true, lock));
                    }
                } finally {
                    fileAccessLock.unlock();
                }
            }
            lock.unlock();
            return Optional.absent();
        } catch (final Throwable e) {
            lock.unlock();
            throw e;
        }
    }

    @Nonnull
    Optional<MultiThreadLock> tryWriteLock() throws IOException {
        final Lock lock = processLocalLock.asWriteLock();
        if (!lock.tryLock()) {
            return Optional.absent();
        }
        Preconditions.checkState(!fileLock.isPresent());
        try {
            fileLock = FileLockUtil.tryWriteLock(path);
        } catch (final Throwable e) {
            lock.unlock();
            throw e;
        }
        if (fileLock.isPresent()) {
            return Optional.<MultiThreadLock>of(new MultiThreadLockImpl(false, lock));
        } else {
            lock.unlock();
            return Optional.absent();
        }
    }
}