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
        return getReadWriteLockImpl(directory.resolve(fileName)).readLock();
    }

    @Nonnull
    public static MultiThreadLock writeLock(@Nonnull final Path directory, @Nonnull final String fileName) throws IOException {
        return getReadWriteLockImpl(directory.resolve(fileName)).writeLock();
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
