package com.indeed.imhotep.fs;

import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author kenh
 */

class LocalCacheLRU {
    private static final Logger LOGGER = Logger.getLogger(LocalCacheLRU.class);

    private final Map<RemoteCachingPath, Pair<Path, Long>> internalCache;
    private final long diskSpaceCapacity;
    private final double purgeWatermark;
    private long diskSpaceUsed = 0;

    LocalCacheLRU(final int initialCapacity, final float loadFactor, final long diskSpaceCapacity, final double purgeWatermark) {
        internalCache = new LinkedHashMap<>(initialCapacity, loadFactor);
        this.diskSpaceCapacity = diskSpaceCapacity;
        this.purgeWatermark = purgeWatermark;
    }

    Path get(final RemoteCachingPath path) {
        final Pair<Path, Long> entry = internalCache.get(path);
        if (entry != null) {
            return entry.getFirst();
        }
        return null;
    }

    void add(final RemoteCachingPath path, final Path cachePath, final long size) {
        LOGGER.debug("Adding cache entry for " + path);
        diskSpaceUsed += purgeWatermark;
        internalCache.put(path, Pair.of(cachePath, size));
        purge();
    }

    void remove(final RemoteCachingPath path) {
        final Pair<Path, Long> entry = internalCache.remove(path);
        if (entry != null) {
            diskSpaceUsed -= entry.getSecond();
            LOGGER.debug("Removed cache entry for " + path);
        }
    }

    private void purge() {
        while (!internalCache.isEmpty() && (diskSpaceUsed > (diskSpaceCapacity * purgeWatermark))) {
            for (final Map.Entry<RemoteCachingPath, Pair<Path, Long>> entry : internalCache.entrySet()) {
                diskSpaceUsed -= entry.getValue().getSecond();
                remove(entry.getKey());
                break;
            }
        }
    }
}
