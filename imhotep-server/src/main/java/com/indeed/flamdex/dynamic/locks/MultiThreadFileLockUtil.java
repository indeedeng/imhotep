package com.indeed.flamdex.dynamic.locks;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

/**
 * @author michihiko
 */
public final class MultiThreadFileLockUtil {
    private MultiThreadFileLockUtil() {

    }

    private static final LoadingCache<String, MultiThreadFileLock> CACHE =
            CacheBuilder.newBuilder()
                    .build(new CacheLoader<String, MultiThreadFileLock>() {
                        @Override
                        public MultiThreadFileLock load(@Nonnull final String path) throws IOException {
                            return new MultiThreadFileLock(Paths.get(path));
                        }
                    });

    @Nonnull
    private static MultiThreadFileLock getReadWriteLockImpl(@Nonnull final Path path) throws IOException {
        final File file = path.toFile();
        //noinspection ResultOfMethodCallIgnored
        file.getParentFile().mkdirs();
        //noinspection ResultOfMethodCallIgnored
        file.createNewFile();
        final String canonicalPath = file.getCanonicalPath();
        try {
            return CACHE.get(canonicalPath);
        } catch (final ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }

    @Nonnull
    public static MultiThreadLock lock(final boolean shared, @Nonnull final Path path) throws IOException {
        if (shared) {
            return getReadWriteLockImpl(path).readLock();
        } else {
            return getReadWriteLockImpl(path).writeLock();
        }
    }

    @Nonnull
    public static MultiThreadLock lock(final boolean shared, @Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return lock(shared, directory.resolve(fileName));
    }

    @Nonnull
    public static MultiThreadLock readLock(@Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return lock(true, directory, fileName);
    }

    @Nonnull
    public static MultiThreadLock writeLock(@Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return lock(false, directory, fileName);
    }

    @Nonnull
    public static Optional<MultiThreadLock> tryWriteLock(@Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return getReadWriteLockImpl(directory.resolve(fileName)).tryWriteLock();
    }
}
