package com.indeed.flamdex.dynamic.locks;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

/**
 * @author michihiko
 */
public final class MultiThreadFileLockUtil {
    private MultiThreadFileLockUtil() {

    }

    private static final LoadingCache<Path, MultiThreadFileLock> CACHE =
            CacheBuilder.newBuilder()
                    .build(new CacheLoader<Path, MultiThreadFileLock>() {
                        @Override
                        public MultiThreadFileLock load(@Nonnull final Path path) throws IOException {
                            return new MultiThreadFileLock(path);
                        }
                    });

    @Nonnull
    private static MultiThreadFileLock getReadWriteLockImpl(@Nonnull final Path path) throws IOException {
        try {
            return CACHE.get(path.normalize());
        } catch (final ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }

    @Nonnull
    public static MultiThreadLock readLock(@Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return getReadWriteLockImpl(directory.resolve(directory.resolve(fileName))).readLock();
    }

    @Nonnull
    public static MultiThreadLock writeLock(@Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return getReadWriteLockImpl(directory.resolve(directory.resolve(fileName))).writeLock();
    }

    @Nonnull
    public static Optional<MultiThreadLock> tryReadLock(@Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return getReadWriteLockImpl(directory.resolve(fileName)).tryReadLock();
    }

    @Nonnull
    public static Optional<MultiThreadLock> tryWriteLock(@Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return getReadWriteLockImpl(directory.resolve(fileName)).tryWriteLock();
    }
}
