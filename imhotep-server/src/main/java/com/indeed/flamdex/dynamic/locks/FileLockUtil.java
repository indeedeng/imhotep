package com.indeed.flamdex.dynamic.locks;

import com.google.common.base.Optional;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper utilities for {@link FileLock}.
 *
 * @author michihiko
 */
public final class FileLockUtil {
    private FileLockUtil() {
    }

    public static class FileLockWithChannel implements Closeable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final FileLock fileLock;

        FileLockWithChannel(@Nonnull final FileLock fileLock) {
            this.fileLock = fileLock;
        }

        @Override
        public void close() throws IOException {
            if (!closed.getAndSet(true)) {
                final FileChannel fileChannel = this.fileLock.channel();
                IOException exception = null;
                try {
                    this.fileLock.close();
                } catch (final IOException e) {
                    exception = e;
                }
                try {
                    fileChannel.close();
                } catch (final IOException e) {
                    if (exception == null) {
                        exception = e;
                    }
                }
                if (exception != null) {
                    throw exception;
                }
            }
        }
    }

    @Nonnull
    private static FileChannel openChannel(final boolean shared, @Nonnull final Path path) throws IOException {
        //noinspection ResultOfMethodCallIgnored
        path.toFile().createNewFile();
        if (shared) {
            return FileChannel.open(path, StandardOpenOption.READ);
        } else {
            return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
    }

    @Nonnull
    public static FileLockWithChannel lock(final boolean shared, @Nonnull final Path path) throws IOException {
        final FileChannel fileChannel = openChannel(shared, path);
        try {
            return new FileLockWithChannel(fileChannel.lock(0, Long.MAX_VALUE, shared));
        } catch (final IOException e) {
            fileChannel.close();
            throw e;
        }
    }

    @Nonnull
    public static FileLockWithChannel lock(final boolean shared, @Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return lock(shared, directory.resolve(fileName));
    }

    @Nonnull
    public static FileLockWithChannel readLock(@Nonnull final Path path) throws IOException {
        return lock(true, path);
    }

    @Nonnull
    public static FileLockWithChannel writeLock(@Nonnull final Path path) throws IOException {
        return lock(false, path);
    }

    @Nonnull
    public static Optional<FileLockWithChannel> tryLock(final boolean shared, @Nonnull final Path path) throws IOException {
        final FileChannel fileChannel = openChannel(shared, path);
        try {
            final FileLock lock = fileChannel.tryLock(0, Long.MAX_VALUE, shared);
            if (lock == null) {
                return Optional.absent();
            } else {
                return Optional.of(new FileLockWithChannel(lock));
            }
        } catch (final IOException e) {
            fileChannel.close();
            throw e;
        }
    }

    @Nonnull
    public static Optional<FileLockWithChannel> tryWriteLock(@Nonnull final Path path) throws IOException {
        return tryLock(false, path);
    }
}
