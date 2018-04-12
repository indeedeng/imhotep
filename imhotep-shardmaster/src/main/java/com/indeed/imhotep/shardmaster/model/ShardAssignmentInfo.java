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

package com.indeed.imhotep.shardmaster.model;

import com.google.common.base.Objects;
import com.indeed.imhotep.client.Host;

/**
 * @author kenh
 */

public class ShardAssignmentInfo {
    private final String dataset;
    private final String shardPath;
    private final Host assignedNode;

    public ShardAssignmentInfo(final String dataset, final String shardPath, final Host assignedNode) {
        this.dataset = dataset;
        this.shardPath = shardPath;
        this.assignedNode = assignedNode;
    }

    public String getDataset() {
        return dataset;
    }

    public String getShardPath() {
        return shardPath;
    }

    public Host getAssignedNode() {
        return assignedNode;
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
        return Objects.equal(dataset, that.dataset) &&
                Objects.equal(shardPath, that.shardPath) &&
                Objects.equal(assignedNode, that.assignedNode);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataset, shardPath, assignedNode);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("dataset", dataset)
                .add("shardPath", shardPath)
                .add("assignedNode", assignedNode)
                .toString();
    }

}
