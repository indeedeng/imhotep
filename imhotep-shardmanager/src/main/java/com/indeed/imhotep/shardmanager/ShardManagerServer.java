package com.indeed.imhotep.shardmanager;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.shardmanager.model.ShardAssignmentInfo;
import com.indeed.imhotep.shardmanager.protobuf.AssignedShard;

/**
 * @author kenh
 */

public class ShardManagerServer implements ShardManager {
    private final ShardAssignmentInfoDao assignmentInfoDao;

    public ShardManagerServer(final ShardAssignmentInfoDao assignmentInfoDao) {
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
