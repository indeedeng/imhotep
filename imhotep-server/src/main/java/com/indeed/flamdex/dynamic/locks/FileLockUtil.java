package com.indeed.flamdex.dynamic.locks;

import com.google.common.base.Optional;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Wrapper utilities for {@link FileLock}.
 *
 * @author michihiko
 */
public final class FileLockUtil {
    private static final Logger LOG = Logger.getLogger(FileLockUtil.class);

    private FileLockUtil() {
    }

    @Nonnull
    private static FileChannel openChannel(final boolean shared, @Nonnull final Path path) throws IOException {
        if (shared) {
            Files.newByteChannel(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE).close();
            return FileChannel.open(path, StandardOpenOption.READ);
        } else {
            return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        }
    }

    @Nonnull
    public static FileChannel lock(final boolean shared, @Nonnull final Path path) throws IOException {
        Files.createDirectories(path.getParent());
        final FileChannel fileChannel = openChannel(shared, path);
        try {
            fileChannel.lock(0, Long.MAX_VALUE, shared);
            return fileChannel;
        } catch (final IOException e) {
            Closeables2.closeQuietly(fileChannel, LOG);
            throw e;
        }
    }

    @Nonnull
    public static FileChannel lock(final boolean shared, @Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return lock(shared, directory.resolve(fileName));
    }

    @Nonnull
    public static FileChannel readLock(@Nonnull final Path path) throws IOException {
        return lock(true, path);
    }

    @Nonnull
    public static FileChannel writeLock(@Nonnull final Path path) throws IOException {
        return lock(false, path);
    }

    @Nonnull
    public static Optional<FileChannel> tryLock(final boolean shared, @Nonnull final Path path) throws IOException {
        final FileChannel fileChannel = openChannel(shared, path);
        try {
            final FileLock lock = fileChannel.tryLock(0, Long.MAX_VALUE, shared);
            if (lock == null) {
                return Optional.absent();
            } else {
                return Optional.of(fileChannel);
            }
        } catch (final IOException e) {
            Closeables2.closeQuietly(fileChannel, LOG);
            throw e;
        }
    }

    @Nonnull
    public static Optional<FileChannel> tryWriteLock(@Nonnull final Path path) throws IOException {
        return tryLock(false, path);
    }
}
