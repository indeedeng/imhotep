package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Wrapper utilities for {@link FileLock}.
 * @author michihiko
 */
final class FileLockUtil {
    private FileLockUtil() {
    }

    @Nonnull
    private static FileChannel openChannel(final boolean shared, @Nonnull final File file) throws IOException {
        //noinspection ResultOfMethodCallIgnored
        file.createNewFile();
        if (shared) {
            return FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } else {
            return FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
    }

    @Nonnull
    static FileLock lock(final boolean shared, @Nonnull final File file) throws IOException {
        final FileChannel fileChannel = openChannel(shared, file);
        try {
            return fileChannel.lock(0, Long.MAX_VALUE, shared);
        } catch (final IOException e) {
            fileChannel.close();
            throw e;
        }
    }

    @Nonnull
    static FileLock lock(final boolean shared, @Nonnull final Path directory, @Nonnull final String... filename) throws IOException {
        return lock(shared, Paths.get(directory.toString(), filename).toFile());
    }

    @Nonnull
    static Optional<FileLock> tryLock(final boolean shared, @Nonnull final File file) throws IOException {
        final FileChannel fileChannel = openChannel(shared, file);
        try {
            return Optional.fromNullable(fileChannel.tryLock(0, Long.MAX_VALUE, shared));
        } catch (final IOException e) {
            fileChannel.close();
            throw e;
        }
    }

    @Nonnull
    static Optional<FileLock> tryLock(final boolean shared, @Nonnull final Path directory, @Nonnull final String... filename) throws IOException {
        return tryLock(shared, Paths.get(directory.toString(), filename).toFile());
    }
}
