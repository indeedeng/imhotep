package com.indeed.imhotep.fs;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import gnu.trove.impl.Constants;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutionException;

/**
 * @author kenh
 */

class LocalFileCache {
    private static final Logger LOGGER = Logger.getLogger(LocalFileCache.class);
    private final Path cacheRootDir;
    private final TObjectIntHashMap<RemoteCachingPath> fileUseCounter = new TObjectIntHashMap<>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, 0);
    private final LoadingCache<RemoteCachingPath, FileCacheEntry> unusedFilesCache;
    private final Object lock = new Object();

    LocalFileCache(final Path cacheRootDir, final long diskSpaceCapacity, final CacheFileLoader cacheFileLoader) {
        this.cacheRootDir = cacheRootDir;
        unusedFilesCache = CacheBuilder.<RemoteCachingPath, FileCacheEntry>newBuilder()
                .maximumWeight(diskSpaceCapacity)
                .weigher(new Weigher<RemoteCachingPath, FileCacheEntry>() {
                    @Override
                    public int weigh(final RemoteCachingPath path, final FileCacheEntry value) {
                        return value.fileSize;
                    }
                })
                .removalListener(new RemovalListener<RemoteCachingPath, FileCacheEntry>() {
                    @Override
                    public void onRemoval(final RemovalNotification<RemoteCachingPath, FileCacheEntry> removalNotification) {
                        if (removalNotification.wasEvicted()) {
                            // we only need to delete the cache if the entry was pushed out
                            final Path cachePath = removalNotification.getValue().cachePath;
                            try {

                                Files.delete(cachePath);
                            } catch (final IOException e) {
                                LOGGER.warn("Failed to delete evicted local cache " + cachePath, e);
                            }
                        }
                    }
                })
                .build(new CacheLoader<RemoteCachingPath, FileCacheEntry>() {
                    @Override
                    public FileCacheEntry load(final RemoteCachingPath path) throws Exception {
                        final Path cachePath = toCachePath(path);
                        final FileCacheEntry entry = cacheFileLoader.load(path, cachePath);
                        Preconditions.checkState(Files.exists(cachePath), "Failed to load " + path + " to cache " + cachePath);
                        return entry;
                    }
                });
    }

    void initialize(final RemoteCachingFileSystem fs) throws IOException {
        synchronized (lock) {
            unusedFilesCache.invalidateAll();
            Files.walkFileTree(cacheRootDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path cachePath, final BasicFileAttributes attrs) throws IOException {
                    super.visitFile(cachePath, attrs);

                    final long localCacheSize = Files.size(cachePath);
                    final RemoteCachingPath path = RemoteCachingPath.resolve(RemoteCachingPath.getRoot(fs), cacheRootDir.relativize(cachePath));

                    unusedFilesCache.put(path, new FileCacheEntry(cachePath, (int) localCacheSize));

                    return FileVisitResult.CONTINUE;
                }
            });
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
    Path cache(final RemoteCachingPath path) throws ExecutionException {
        Preconditions.checkArgument(path.isAbsolute(), "Only absolute paths are supported");
        synchronized (lock) {
            return unusedFilesCache.get(path).cachePath;
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
        synchronized (lock) {
            // this unnatural code is necessary to get the cached file, or force a cache if not present
            final FileCacheEntry fileCacheEntry = unusedFilesCache.get(path);
            unusedFilesCache.invalidate(path);
            fileUseCounter.adjustOrPutValue(path, 1, 1);
            return fileCacheEntry.cachePath;
        }
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
            final int counter = fileUseCounter.adjustOrPutValue(path, -1, 0);
            if (counter == 0) {
                fileUseCounter.remove(path);
                try {
                    unusedFilesCache.put(path, new FileCacheEntry(cachePath, (int) Files.size(cachePath)));
                } catch (final IOException e) {
                    LOGGER.warn("Failed to get file size for disposed cache file " + cachePath +
                            ". The file will be assumed to been removed", e);
                }
            }
        }
    }

    static class FileCacheEntry {
        private final Path cachePath;
        private final int fileSize;

        FileCacheEntry(final Path cachePath, final int fileSize) {
            this.cachePath = cachePath;
            this.fileSize = fileSize;
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
        FileCacheEntry load(final RemoteCachingPath src, final Path dest) throws IOException;
    }
}
