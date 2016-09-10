package com.indeed.imhotep.shardmanager.model;

import com.google.common.base.Objects;

/**
 * @author kenh
 */

public class ShardAssignmentInfo {
    private final String dataset;
    private final String shardId;
    private final String shardPath;
    private final String assignedNode;

    public ShardAssignmentInfo(final String dataset, final String shardId, final String shardPath, final String assignedNode) {
        this.dataset = dataset;
        this.shardId = shardId;
        this.shardPath = shardPath;
        this.assignedNode = assignedNode;
    }

    public String getDataset() {
        return dataset;
    }

    public String getShardId() {
        return shardId;
    }

    public String getShardPath() {
        return shardPath;
    }

    public String getAssignedNode() {
        return assignedNode;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ShardAssignmentInfo that = (ShardAssignmentInfo) o;
        return Objects.equal(dataset, that.dataset) &&
                Objects.equal(shardId, that.shardId) &&
                Objects.equal(shardPath, that.shardPath) &&
                Objects.equal(assignedNode, that.assignedNode);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataset, shardId, shardPath, assignedNode);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("dataset", dataset)
                .add("shardId", shardId)
                .add("shardPath", shardPath)
                .add("assignedNode", assignedNode)
                .toString();
    }

}
