package com.indeed.imhotep.fs;

/**
 * @author xweng
 */
public class CachedDatasetSnapshot {
    public final String dataset;
    public final int cachedSizeMB;
    public final int cachedShardCount;

    CachedDatasetSnapshot(final String dataset, final int cachedSizeMB, final int cachedShardCount) {
        this.dataset = dataset;
        this.cachedSizeMB = cachedSizeMB;
        this.cachedShardCount = cachedShardCount;
    }
}
