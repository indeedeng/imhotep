package com.indeed.imhotep.shardmanager.model;

import com.google.common.base.Objects;

/**
 * @author kenh
 */

public class ShardAssignmentInfo {
    private final String dataset;
    private final String shardId;
    private final long version;
    private final String assignedNode;

    public ShardAssignmentInfo(final String dataset, final String shardId, final long version, final String assignedNode) {
        this.dataset = dataset;
        this.shardId = shardId;
        this.version = version;
        this.assignedNode = assignedNode;
    }

    public String getDataset() {
        return dataset;
    }

    public String getShardId() {
        return shardId;
    }

    public long getVersion() {
        return version;
    }

    public String getAssignedNode() {
        return assignedNode;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("dataset", dataset)
                .add("shardId", shardId)
                .add("version", version)
                .add("assignedNode", assignedNode)
                .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShardAssignmentInfo)) {
            return false;
        }
        final ShardAssignmentInfo that = (ShardAssignmentInfo) o;
        return version == that.version &&
                Objects.equal(dataset, that.dataset) &&
                Objects.equal(shardId, that.shardId) &&
                Objects.equal(assignedNode, that.assignedNode);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataset, shardId, version, assignedNode);
    }
}
