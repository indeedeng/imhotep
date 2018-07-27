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

package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;

/**
 * @author kenh
 */

public class DatabaseShardMaster implements ShardMaster {
    private final ShardAssignmentInfoDao assignmentInfoDao;

    public DatabaseShardMaster(final ShardAssignmentInfoDao assignmentInfoDao) {
        this.assignmentInfoDao = assignmentInfoDao;
    }

    @Override
    public Iterable<AssignedShard> getAssignments(final Host node) {
        return FluentIterable
                .from(assignmentInfoDao.getAssignments(node))
                .transform(new Function<ShardAssignmentInfo, AssignedShard>() {
                    @Override
                    public AssignedShard apply(final ShardAssignmentInfo shard) {
                        return AssignedShard.newBuilder()
                                .setDataset(shard.getDataset())
                                .setShardPath(shard.getShardPath())
                                .build();
                    }
                });
    }
}
