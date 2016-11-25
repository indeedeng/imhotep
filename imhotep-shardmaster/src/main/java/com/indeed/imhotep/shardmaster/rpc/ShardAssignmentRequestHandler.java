package com.indeed.imhotep.shardmaster.rpc;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * @author kenh
 */

class ShardAssignmentRequestHandler implements RequestHandler {
    private final ShardMaster shardMaster;
    private final int responseBatchSize;

    ShardAssignmentRequestHandler(final ShardMaster shardMaster, final int responseBatchSize) {
        this.shardMaster = shardMaster;
        this.responseBatchSize = responseBatchSize;
    }

    @Override
    public Iterable<ShardMasterResponse> handleRequest(final ShardMasterRequest request) {
        final Host node = new Host(request.getNode().getHost(), request.getNode().getPort());
        final Iterable<AssignedShard> assignedShards;
        try {
            assignedShards = shardMaster.getAssignments(node);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get shards for " + node, e);
        }

        return Iterables.transform(
                Iterables.partition(assignedShards, responseBatchSize),
                new Function<List<AssignedShard>, ShardMasterResponse>() {
                    @Override
                    public ShardMasterResponse apply(@Nullable final List<AssignedShard> shards) {
                        return ShardMasterResponse.newBuilder()
                                .setResponseCode(ShardMasterResponse.ResponseCode.OK)
                                .addAllAssignedShards(shards)
                                .build();
                    }
                });
    }
}
