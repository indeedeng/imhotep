package com.indeed.imhotep.service;

import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class ShardId {

    private static final Logger log = Logger.getLogger(ShardId.class);

    private final String dataset;

    private final String id;

    private final long shardVersion;

    private final String indexDir;

    public ShardId(final String dataset, final String shardId, final long shardVersion, final String indexDir) {
        this.dataset = dataset;
        this.id = shardId;
        this.shardVersion = shardVersion;
        this.indexDir = indexDir;
    }

    public String getDataset() {
        return dataset;
    }

    public String getId() {
        return id;
    }

    public long getShardVersion() {
        return shardVersion;
    }

    public String getIndexDir() {
        return indexDir;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ShardId shardId1 = (ShardId)o;
        if (shardVersion != shardId1.shardVersion) {
            return false;
        }
        if (dataset != null ? !dataset.equals(shardId1.dataset) : shardId1.dataset != null) {
            return false;
        }
        if (indexDir != null ? !indexDir.equals(shardId1.indexDir) : shardId1.indexDir != null) {
            return false;
        }
        if (id != null ? !id.equals(shardId1.id) : shardId1.id != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = dataset != null ? dataset.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (int)(shardVersion ^ (shardVersion >>> 32));
        result = 31 * result + (indexDir != null ? indexDir.hashCode() : 0);
        return result;
    }
}
