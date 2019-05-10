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

package com.indeed.imhotep.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.AbstractCache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.imhotep.service.MetricStatsEmitter;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author kenh
 */

class LocalFileCache {
    private static final Logger LOGGER = Logger.getLogger(LocalFileCache.class);
    private final Path cacheRootDir;
    private final long diskSpaceCapacity;
    private final int diskBlockSize;
    private final AtomicLong diskSpaceUsage = new AtomicLong(0);
    private final AtomicLong referencedFilesCacheSize = new AtomicLong(0);
    private final Object2IntOpenHashMap<RemoteCachingPath> fileUseCounter;
    private final UnusedFileCache unusedFilesCache;
    private final LoadingCache<RemoteCachingPath, FileCacheEntry> referencedFilesCache;
    private final Object lock = new Object();
    private final MetricStatsEmitter statsEmitter;
    private static final int FAST_REPORTING_FREQUENCY_MILLIS = 100;
    private static final int SLOW_REPORTING_FREQUENCY_MINUTES = 60;
    // whether load already cached files under the cacheRootDir when LocalFileCache is initialized
    private final boolean loadExistingFiles;

    @VisibleForTesting
    LocalFileCache(
            final RemoteCachingFileSystem fs,
            final Path cacheRootDir,
            final long diskSpaceCapacity,
            final int diskBlockSize,
            final CacheFileLoader cacheFileLoader) throws IOException {
        this(fs, cacheRootDir, diskSpaceCapacity, diskBlockSize, cacheFileLoader, MetricStatsEmitter.NULL_EMITTER, "test", true);
    }

    LocalFileCache(
            final RemoteCachingFileSystem fs,
            final Path cacheRootDir,
            final long diskSpaceCapacity,
            final int diskBlockSize,
            final CacheFileLoader cacheFileLoader,
            final MetricStatsEmitter statsEmitter,
            final String statsTypePrefix,
            final boolean loadExistingFiles) throws IOException {
        this.cacheRootDir = cacheRootDir;
        this.diskSpaceCapacity = diskSpaceCapacity;
        this.loadExistingFiles = loadExistingFiles;
        this.statsEmitter = statsEmitter;

        final CacheStatsEmitter cacheFileStatsEmitter = new CacheStatsEmitter(statsTypePrefix);
        final ScheduledExecutorService fastStatsReportingExecutor =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("sumCacheOldestFileReferencedFilesSizeReporter").setDaemon(true).build());
        fastStatsReportingExecutor.scheduleAtFixedRate(cacheFileStatsEmitter::reportFastFreqStats, FAST_REPORTING_FREQUENCY_MILLIS, FAST_REPORTING_FREQUENCY_MILLIS, TimeUnit.MILLISECONDS);
        final ScheduledExecutorService slowStatsReportingExecutor =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("sumCacheFileSizeReporter").setDaemon(true).build());
        slowStatsReportingExecutor.scheduleAtFixedRate(cacheFileStatsEmitter::reportSumCacheFileSize, SLOW_REPORTING_FREQUENCY_MINUTES, SLOW_REPORTING_FREQUENCY_MINUTES, TimeUnit.MINUTES);

        this.diskBlockSize = diskBlockSize;

        fileUseCounter = new Object2IntOpenHashMap<>();
        fileUseCounter.defaultReturnValue(0);

        unusedFilesCache = new UnusedFileCache();

        referencedFilesCache = CacheBuilder.newBuilder()
                .build(new CacheLoader<RemoteCachingPath, FileCacheEntry>() {
                    @Override
                    public FileCacheEntry load(@Nonnull final RemoteCachingPath path) throws IOException {
                        final Path cachePath = toCachePath(path);
                        final Path cacheParentPath = cachePath.getParent();

                        Files.createDirectories(cacheParentPath);

                        final Path tempPath = cacheParentPath.resolve(UUID.randomUUID().toString());
                        cacheFileLoader.load(path, tempPath);

                        // only sane way that I can come up with to do replace existing atomically
                        if (!tempPath.toFile().renameTo(cachePath.toFile())) {
                            //noinspection ResultOfMethodCallIgnored
                            tempPath.toFile().delete();
                            throw new IOException("Failed to place cache file under " + cachePath);
                        }


                        final long fileSize = sizeOnDisk(cachePath);
                        final int lastAccessEpochTimeSeconds = (int)(System.currentTimeMillis()/1000);

                        diskSpaceUsage.addAndGet(fileSize);
                        referencedFilesCacheSize.addAndGet(fileSize);
                        unusedFilesCache.cleanUp();

                        return new FileCacheEntry(
                                cachePath,
                                fileSize,
                                lastAccessEpochTimeSeconds
                        );
                    }
                });

        initialize(fs);

    }


    private class CacheStatsEmitter{
        private final String statsTypePrefix;

        public CacheStatsEmitter(final String statsTypePrefix) {
            this.statsTypePrefix = statsTypePrefix;
        }

        private SumCacheFileSizeIntervals getSumCacheFilesSize() {
            final SumCacheFileSizeIntervals sumCacheFileStats;
            final long sumFileSizesReferenceCache;

            synchronized (lock) {
                sumFileSizesReferenceCache = referencedFilesCache.asMap().values().stream().mapToLong(x -> x.fileSize).sum();
                sumCacheFileStats = unusedFilesCache.getSumCacheFileSizeStats();
            }

            //we consider Referenced file to have accessTime = System.currentTime
            sumCacheFileStats.minuteSum += sumFileSizesReferenceCache;
            sumCacheFileStats.hourSum += sumFileSizesReferenceCache;
            sumCacheFileStats.minuteSum += sumFileSizesReferenceCache;

            return sumCacheFileStats;
        }

        public void reportFastFreqStats() { // reports total cache size, sum of size of referenced files, oldest file age
            statsEmitter.histogram(statsTypePrefix + ".total.size", getCacheUsage());
            int oldestFileAgeSeconds = unusedFilesCache.getOldestFileAccessTime();

            if (oldestFileAgeSeconds != Integer.MIN_VALUE) {  // ignoring the files never accessed.
                int currentTimeMinutes = (int)TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
                int minutesSinceLastAccess = currentTimeMinutes - (oldestFileAgeSeconds/60);
                statsEmitter.histogram(statsTypePrefix + ".oldest.file.age.minutes", minutesSinceLastAccess);
            }

            statsEmitter.histogram(statsTypePrefix + ".referenced.files.size", referencedFilesCacheSize.get());
        }

        public void reportSumCacheFileSize() {
            SumCacheFileSizeIntervals sumCacheFilesStats = getSumCacheFilesSize();

            statsEmitter.gauge(statsTypePrefix + ".sum.size.minute", sumCacheFilesStats.minuteSum);
            statsEmitter.gauge(statsTypePrefix + ".sum.size.hour", sumCacheFilesStats.hourSum);
            statsEmitter.gauge(statsTypePrefix + ".sum.size.day", sumCacheFilesStats.daySum);
        }
    }



    private void evictCacheFile(final FileCacheEntry entry) {
        // we only need to delete the cache if the entry was pushed out
        final Path cachePath = entry.cachePath;
        try {
            Files.delete(cachePath);
            diskSpaceUsage.addAndGet(-entry.fileSize);
        } catch (final IOException e) {
            LOGGER.error("Failed to delete evicted local cache " + cachePath, e);
        }
    }

    private void initialize(final RemoteCachingFileSystem fs) throws IOException {
        Files.createDirectories(cacheRootDir);
        synchronized (lock) {
            unusedFilesCache.invalidateAll();
            if (loadExistingFiles) {
                Files.walkFileTree(cacheRootDir, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(final Path cachePath, final BasicFileAttributes attrs) throws IOException {
                        super.visitFile(cachePath, attrs);

                        final long localCacheSize = sizeOnDisk(cachePath);
                        final int lastAccessEpochTimeSeconds = Integer.MIN_VALUE; // Initialize with MIN_VALUE to ignore in stats reporting.
                        final RemoteCachingPath path = RemoteCachingPath.resolve(RemoteCachingPath.getRoot(fs), cacheRootDir.relativize(cachePath));

                        diskSpaceUsage.addAndGet(localCacheSize);
                        unusedFilesCache.put(path, new FileCacheEntry(cachePath, localCacheSize, lastAccessEpochTimeSeconds));

                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                        // Delete empty directories separately since it's not handled by cleanUp() below
                        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir)) {
                            final boolean dirIsEmpty = !dirStream.iterator().hasNext();
                            if (dirIsEmpty) {
                                Files.delete(dir);
                            }
                        }
                        return super.postVisitDirectory(dir, exc);
                    }
                });
            } else {
                // we don't know the host information from files in disk, so won't load them for peer to peer cache.
                FileUtils.deleteDirectory(cacheRootDir.toFile());
            }
            // recreate in case it was empty and got deleted above
            Files.createDirectories(cacheRootDir);
            // force clean up
            unusedFilesCache.cleanUp();
        }
    }

    private Path toCachePath(final RemoteCachingPath path) {
        final RemoteCachingPath relativePath = path.asRelativePath();
        return RemoteCachingPath.resolve(cacheRootDir, relativePath);
    }

    /**
     * Used to locally cache the file corresponding to the {@link RemoteCachingPath}.
     * WARNING: This does not affect the usage refcount, so there is no guarantee on how long the file is retained
     *
     * @param path the remote path you want to cache locally
     * @return the path corresponding to the local cache file
     */
    Path cache(final RemoteCachingPath path) throws ExecutionException, IOException {
        Preconditions.checkArgument(path.isAbsolute(), "Only absolute paths are supported");

        if (Files.isDirectory(path)) {
            // directories should not go into the usage book keeping
            final Path cacheDirPath = toCachePath(path);
            Files.createDirectories(cacheDirPath);
            return cacheDirPath;
        } else {
            try (final ScopedCacheFile openedCacheFile = getForOpen(path)) {
                return openedCacheFile.getCachePath();
            }
        }
    }

    /**
     * get the local cache disk space usage
     * @return the usage in bytes
     */
    long getCacheUsage() {
        return diskSpaceUsage.get();
    }

    private void incFileUsageRef(final RemoteCachingPath path) {
        fileUseCounter.add(path, 1);
    }

    private int decFileUsageRef(final RemoteCachingPath path) {
        final int count = fileUseCounter.add(path, -1);
        if (count == 1) {
            fileUseCounter.remove(path);
        }
        return count - 1;
    }

    /**
     * Used to locally open the path pointed to by {@link RemoteCachingPath}.
     * This will return a path to the local cache and increment the usage refcount so it will not be evicted
     * The caller is responsible for calling {@link #dispose(RemoteCachingPath, Path)} after usage
     *
     * @param path the remote path you want to access for opening
     * @return the path corresponding to the local cache file
     */
    Path get(final RemoteCachingPath path) throws ExecutionException {
        FileCacheEntry fileCacheEntry;
        synchronized (lock) {
            incFileUsageRef(path);
            fileCacheEntry = unusedFilesCache.getIfPresent(path);
            if (fileCacheEntry != null) {
                // if the file was in the unused file cache, we can use that again
                unusedFilesCache.invalidate(path);
                referencedFilesCache.put(path, fileCacheEntry);
                referencedFilesCacheSize.addAndGet(fileCacheEntry.fileSize);
                return fileCacheEntry.cachePath;
            }
        }

        // at this point, unusedFilesCache does not contain path so we are
        // 1. getting the cached value
        // 2. loading the cache file (which can result in exceptions and the file use counter has to be rolled back)
        try {
            try (final Closeable ignored = TaskScheduler.CPUScheduler.temporaryUnlock()) {
                fileCacheEntry = referencedFilesCache.get(path) ;
            } catch (final IOException e) {
                throw Throwables.propagate(e);
            }
        } catch (final Throwable e) {
            synchronized (lock) {
                decFileUsageRef(path);
            }
            throw e;
        }
        return fileCacheEntry.cachePath;
    }

    /**
     * Used to locally open the path pointed to by {@link RemoteCachingPath}.
     *
     * @param path the remote path you want to access for opening
     * @return A scoped object that must be closed after usage
     */
    ScopedCacheFile getForOpen(final RemoteCachingPath path) throws ExecutionException {
        Preconditions.checkArgument(path.isAbsolute(), "Only absolute paths are supported");
        return new ScopedCacheFile(this, path);
    }

    /**
     * Decrement the usage refcount for the cache file.
     * The cache may be eligible for eviction after this call
     *
     * @param path      the remote path you want to give up access
     * @param cachePath the corresponding local cache file
     */
    void dispose(final RemoteCachingPath path, final Path cachePath) {
        synchronized (lock) {
            final int counter = decFileUsageRef(path);
            if (counter == 0) {
                FileCacheEntry referencedFile = referencedFilesCache.getIfPresent(path);
                if (referencedFile != null) {
                    long referencedFileSize = referencedFile.fileSize;
                    referencedFilesCache.invalidate(path);
                    referencedFilesCacheSize.addAndGet(-referencedFileSize);
                    unusedFilesCache.put(path, new FileCacheEntry(cachePath, referencedFileSize, (int) (System.currentTimeMillis() / 1000)));
                } else {
                    LOGGER.error("Failed to get file" + cachePath + "from referencedFilesCache while disposing. " +
                            "The file will assumed to be removed and not considered for disposing unless referenced again. ");
                }
            }
        }
    }

    private long sizeOnDisk(final Path path) throws IOException {
        final long size = Files.size(path);
        final long sizeOnDisk = (size + diskBlockSize - 1) / diskBlockSize * diskBlockSize;
        return sizeOnDisk;
    }

    static class FileCacheEntry {
        private final Path cachePath;
        private final long fileSize;
        private final int lastAccessEpochTimeSeconds;

        private FileCacheEntry(final Path cachePath, final long fileSize, final int lastAccessEpochTimeSeconds) {
            this.cachePath = cachePath;
            this.fileSize = fileSize;
            this.lastAccessEpochTimeSeconds = lastAccessEpochTimeSeconds;
        }
    }

    static class SumCacheFileSizeIntervals {
        private long minuteSum;
        private long hourSum;
        private long daySum;

        public SumCacheFileSizeIntervals() {
            this.minuteSum = 0;
            this.hourSum = 0;
            this.daySum = 0;
        }

    }

    interface CacheFileLoader {
        void load(final RemoteCachingPath src, final Path dest) throws IOException;
    }

    private class UnusedFileCache extends AbstractCache<RemoteCachingPath, FileCacheEntry> {
        private final LinkedHashMap<RemoteCachingPath, FileCacheEntry> updateOrderMap = new LinkedHashMap<>();

        @Override
        public synchronized void cleanUp() {
            while ((diskSpaceUsage.get() > diskSpaceCapacity) && !updateOrderMap.isEmpty()) {
                final Iterator<FileCacheEntry> iterator = updateOrderMap.values().iterator();
                final FileCacheEntry entry = iterator.next();
                evictCacheFile(entry);
                iterator.remove();
            }
        }

        @Override
        public synchronized void put(@Nonnull final RemoteCachingPath key, @Nonnull final FileCacheEntry value) {
            updateOrderMap.remove(key);
            updateOrderMap.put(key, value);

            cleanUp();
        }

        @Override
        public synchronized void invalidateAll() {
            updateOrderMap.clear();
        }

        @Override
        public synchronized void invalidate(final Object key) {
            updateOrderMap.remove(key);
        }

        @Override
        public synchronized FileCacheEntry getIfPresent(@Nonnull final Object key) {
            return updateOrderMap.get(key);
        }

        public synchronized int getOldestFileAccessTime() {
            return updateOrderMap.values().stream().mapToInt(x -> x.lastAccessEpochTimeSeconds).findFirst().orElse(Integer.MIN_VALUE);
        }

        public synchronized SumCacheFileSizeIntervals getSumCacheFileSizeStats() {

            final Iterator<FileCacheEntry> iterator = updateOrderMap.values().iterator();
            final int secondsSinceEpoch = (int)(System.currentTimeMillis()/1000);
            SumCacheFileSizeIntervals sumCacheFileStats = new SumCacheFileSizeIntervals();
            while(iterator.hasNext()) {
                final FileCacheEntry entry = iterator.next();
                if (entry.lastAccessEpochTimeSeconds == Integer.MIN_VALUE) { // ignore files not accessed yet
                    continue;
                }
                final int timeDifferenceSeconds = secondsSinceEpoch - entry.lastAccessEpochTimeSeconds;

                if (timeDifferenceSeconds <= 60) {
                    sumCacheFileStats.minuteSum += entry.fileSize;
                }
                if (timeDifferenceSeconds <= 60*60 ) {
                    sumCacheFileStats.hourSum += entry.fileSize;
                }
                if (timeDifferenceSeconds <= 60*60*24) {
                    sumCacheFileStats.daySum += entry.fileSize;
                }
            }

            return sumCacheFileStats;
        }

    }
}