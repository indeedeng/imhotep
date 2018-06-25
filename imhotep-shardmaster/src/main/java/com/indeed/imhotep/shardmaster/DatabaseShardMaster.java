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

import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author kenh
 */

public class DatabaseShardMaster implements ShardMaster {
    private final ShardAssignmentInfoDao assignmentInfoDao;
    private final AtomicBoolean initializationComplete;

    public DatabaseShardMaster(final ShardAssignmentInfoDao assignmentInfoDao, final AtomicBoolean initializationComplete) {
        this.assignmentInfoDao = assignmentInfoDao;
        this.initializationComplete = initializationComplete;
    }

    @Override
    public Iterable<AssignedShard> getAssignments(final Host node) {
        if (!initializationComplete.get()) {
            throw new IllegalStateException("Initialization is not yet complete, please wait");
        }

        return StreamSupport.stream(assignmentInfoDao.getAssignments(node).spliterator(), true).map(shard -> AssignedShard.newBuilder()
                .setDataset(shard.getDataset())
                .setShardPath(shard.getShardPath())
                .build()).collect(Collectors.toList());
    }
}
