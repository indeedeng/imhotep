package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
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
    public Iterable<AssignedShard> getAssignments(final String node) {
        return FluentIterable
                .from(assignmentInfoDao.getAssignments(node))
                .transform(new Function<ShardAssignmentInfo, AssignedShard>() {
                    @Override
                    public AssignedShard apply(final ShardAssignmentInfo shard) {
                        return AssignedShard.newBuilder()
                                .setDataset(shard.getDataset())
                                .setShardId(shard.getShardId())
                                .setShardPath(shard.getShardPath())
                                .build();
                    }
                });
    }
}
