package com.indeed.imhotep.fs;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

/**
 * @author xweng
 */
class ScopedCacheFile implements Closeable {
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