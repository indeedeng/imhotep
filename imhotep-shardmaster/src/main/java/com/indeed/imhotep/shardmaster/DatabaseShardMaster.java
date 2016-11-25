package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;

import java.util.concurrent.atomic.AtomicBoolean;

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
