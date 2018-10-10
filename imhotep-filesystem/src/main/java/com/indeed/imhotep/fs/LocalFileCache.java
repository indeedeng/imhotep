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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.Pair;
import com.indeed.util.core.threads.NamedThreadFactory;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Minutes;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.max;


/**
 * @author kenh
 */

class LocalFileCache {
    private static final Logger LOGGER = Logger.getLogger(LocalFileCache.class);
    private final Path cacheRootDir;
    private final long diskSpaceCapacity;
    private final int diskBlockSize;
    private final AtomicLong diskSpaceUsage = new AtomicLong(0);
    private final Object2IntOpenHashMap<RemoteCachingPath> fileUseCounter;
    private final Cache<RemoteCachingPath, FileCacheEntry> unusedFilesCache;
    private final LoadingCache<RemoteCachingPath, FileCacheEntry> referencedFilesCache;
    private final Object lock = new Object();
    private final MetricStatsEmitter statsEmitter;
    private static final int REPORTING_FREQUENCY_MILLIS = 1000;

    LocalFileCache(final RemoteCachingFileSystem fs, final Path cacheRootDir, final long diskSpaceCapacity, final int diskBlockSize, final CacheFileLoader cacheFileLoader) throws IOException {
        this(fs, cacheRootDir, diskSpaceCapacity, MetricStatsEmitter.NULL_EMITTER, diskBlockSize, cacheFileLoader);
    }

    LocalFileCache(final RemoteCachingFileSystem fs, final Path cacheRootDir, final long diskSpaceCapacity, final MetricStatsEmitter statsEmitter, final int diskBlockSize, final CacheFileLoader cacheFileLoader) throws IOException {
        this.cacheRootDir = cacheRootDir;
        this.diskSpaceCapacity = diskSpaceCapacity;
        this.statsEmitter = statsEmitter;

        CacheStatsEmitter cacheFileStatsEmitter = new CacheStatsEmitter();
        final ScheduledExecutorService statsReportingExecutor =
                                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("filecacheStatsReporter"));
        statsReportingExecutor.scheduleAtFixedRate(cacheFileStatsEmitter::setAndReportStats, REPORTING_FREQUENCY_MILLIS, REPORTING_FREQUENCY_MILLIS, TimeUnit.MILLISECONDS);

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
                        final int lastAccessTimeSecondsSinceEpoch = getAccessTimeSecondsEpochFromPath(cachePath);

                        diskSpaceUsage.addAndGet(fileSize);
                        unusedFilesCache.cleanUp();

                        return new FileCacheEntry(
                                cachePath,
                                fileSize,
                                lastAccessTimeSecondsSinceEpoch
                        );
                    }
                });

        initialize(fs);

    }

    private class CacheStatsEmitter{
        private Map<Pair<Integer,String>, Long> sumCacheFileSizeValues = new HashMap<>();
        private Iterator<Map.Entry<RemoteCachingPath,FileCacheEntry>> unusedCacheFilesIterator;
        private Iterator<Map.Entry<RemoteCachingPath,FileCacheEntry>> referencedCacheFilesIterator;
        private DateTime statsStartTime;

        CacheStatsEmitter(){
            sumCacheFileSizeValues.put(new Pair<>(1,"minute"), 0L);
            sumCacheFileSizeValues.put(new Pair<>(1*60,"hour"), 0L);
            sumCacheFileSizeValues.put(new Pair<>(1*60*24,"day"), 0L);
        }

        public void setStatsStartTime(DateTime statsStartTime) {
            this.statsStartTime = statsStartTime;
        }

        public void setCacheFilesIterators() {
            unusedCacheFilesIterator =  ((UnusedFileCache)unusedFilesCache).entrySetIterator();
            referencedCacheFilesIterator = referencedFilesCache.asMap().entrySet().iterator();
        }

        private long getOldestFileAgeSetSumCacheFileSize(){

            setCacheFilesIterators();
            sumCacheFileSizeValues.replaceAll((k,v) -> 0L);
            int oldestFileAgeMinutes = Integer.MAX_VALUE;
            while (unusedCacheFilesIterator.hasNext() || referencedCacheFilesIterator.hasNext()){
                Map.Entry<RemoteCachingPath, FileCacheEntry> entry;
                if (unusedCacheFilesIterator.hasNext()) {
                    entry = unusedCacheFilesIterator.next();
                } else {
                    entry = referencedCacheFilesIterator.next();
                }

                int lastAccessTimeDifferenceMinutes = Minutes.minutesBetween(new DateTime((long)entry.getValue().lastAccessTimeSecondsSinceEpoch*1000),statsStartTime).getMinutes();
                oldestFileAgeMinutes = max(oldestFileAgeMinutes, lastAccessTimeDifferenceMinutes);

                for (Map.Entry<Pair<Integer,String>,Long> durationEntry : sumCacheFileSizeValues.entrySet()){
                    if (lastAccessTimeDifferenceMinutes <= durationEntry.getKey().getFirst()){
                        sumCacheFileSizeValues.put(durationEntry.getKey(),durationEntry.getValue() + entry.getValue().fileSize);
                    }
                }
            }

            return oldestFileAgeMinutes;

        }

        private void reportStats(long oldestFileAgeMinuteDifference){
            statsEmitter.histogram("filecache.totalcachesize",  getCacheUsage());
            statsEmitter.histogram("filecache.oldestFileAgeMinutes", oldestFileAgeMinuteDifference);

            for (Map.Entry<Pair<Integer,String>,Long> statsEntry : sumCacheFileSizeValues.entrySet()){
                statsEmitter.histogram("fileCache.sumFileSizeLast"+ statsEntry.getKey().getSecond() + "AccessKB", (double)statsEntry.getValue()/1000);
            }

        }

        public void setAndReportStats(){
            setStatsStartTime(new DateTime(System.currentTimeMillis()));
            long oldestFileAgeMinuteDifference = getOldestFileAgeSetSumCacheFileSize();
            reportStats(oldestFileAgeMinuteDifference);
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
            Files.walkFileTree(cacheRootDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path cachePath, final BasicFileAttributes attrs) throws IOException {
                    super.visitFile(cachePath, attrs);

                    final long localCacheSize = sizeOnDisk(cachePath);
                    final int lastAccessTimeSecondsSinceEpoch = getAccessTimeSecondsEpochFromPath(cachePath);
                    final RemoteCachingPath path = RemoteCachingPath.resolve(RemoteCachingPath.getRoot(fs), cacheRootDir.relativize(cachePath));

                    diskSpaceUsage.addAndGet(localCacheSize);
                    unusedFilesCache.put(path, new FileCacheEntry(cachePath, (int) localCacheSize, lastAccessTimeSecondsSinceEpoch));

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
                return openedCacheFile.cachePath;
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

    public static int getAccessTimeSecondsEpochFromPath(Path filePath){
        try {
            int AccessTimeSecondsEpoch = (int) Files.readAttributes(filePath, BasicFileAttributes.class).lastAccessTime().to(TimeUnit.SECONDS);
            return AccessTimeSecondsEpoch;
        } catch (IOException e) {
            return Integer.MAX_VALUE;
        }

    }

    /**
     * Used to locally open the path pointed to by {@link RemoteCachingPath}.
     * This will return a path to the local cache and increment the usage refcount so it will not be evicted
     * The caller is responsible for calling {@link #dispose(RemoteCachingPath, Path)} after usage
     *
     * @param path the remote path you want to access for opening
     * @return the path corresponding to the local cache file
     */
    private Path get(final RemoteCachingPath path) throws ExecutionException {
        FileCacheEntry fileCacheEntry;
        synchronized (lock) {
            incFileUsageRef(path);
            fileCacheEntry = unusedFilesCache.getIfPresent(path);
            if (fileCacheEntry != null) {
                // if the file was in the unused file cache, we can use that again
                unusedFilesCache.invalidate(path);
                referencedFilesCache.put(path, fileCacheEntry);
                return fileCacheEntry.cachePath;
            }
        }

        // at this point, unusedFilesCache does not contain path so we are
        // 1. getting the cached value
        // 2. loading the cache file (which can result in exceptions and the file use counter has to be rolled back)
        try {
            try (final Closeable ignored = TaskScheduler.CPUScheduler.temporaryUnlock()) {
                fileCacheEntry = referencedFilesCache.get(path);
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
    private void dispose(final RemoteCachingPath path, final Path cachePath) {
        synchronized (lock) {
            final int counter = decFileUsageRef(path);
            if (counter == 0) {
                referencedFilesCache.invalidate(path);
                try {
                    unusedFilesCache.put(path, new FileCacheEntry(cachePath, sizeOnDisk(cachePath), getAccessTimeSecondsEpochFromPath(cachePath)));
                } catch (final IOException e) {
                    LOGGER.warn("Failed to get file size for disposed cache file " + cachePath +
                            ". The file will be assumed to been removed", e);
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
        private final int lastAccessTimeSecondsSinceEpoch;

        FileCacheEntry(final Path cachePath, final long fileSize, final int lastAccessTimeSecondsSinceEpoch) {
            this.cachePath = cachePath;
            this.fileSize = fileSize;
            this.lastAccessTimeSecondsSinceEpoch = lastAccessTimeSecondsSinceEpoch;
        }
    }

    static class ScopedCacheFile implements Closeable {
        private final LocalFileCache cache;
        private final RemoteCachingPath path;
        private final Path cachePath;

        ScopedCacheFile(final LocalFileCache cache, final RemoteCachingPath path) throws ExecutionException {
            this.cache = cache;
            this.path = path;
            cachePath = cache.get(path);
        }

        Path getCachePath() {
            return cachePath;
        }

        @Override
        public void close() {
            cache.dispose(path, cachePath);
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

        public synchronized Iterator<Map.Entry<RemoteCachingPath,FileCacheEntry>> entrySetIterator(){
            return updateOrderMap.entrySet().iterator();
        }
    }
}
