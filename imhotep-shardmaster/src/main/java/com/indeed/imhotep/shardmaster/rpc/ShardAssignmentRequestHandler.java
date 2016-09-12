package com.indeed.imhotep.shardmaster.rpc;

import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;

import java.io.IOException;

/**
 * @author kenh
 */

class ShardAssignmentRequestHandler implements RequestHandler {
    private final ShardMaster shardMaster;

    ShardAssignmentRequestHandler(final ShardMaster shardMaster) {
        this.shardMaster = shardMaster;
    }

    @Override
    public ShardMasterResponse handleRequest(final ShardMasterRequest request) {
        final String node = request.getNode().getHost();
        final Iterable<AssignedShard> assignedShards;
        try {
            assignedShards = shardMaster.getAssignments(node);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get shards for " + node, e);
        }

        return ShardMasterResponse.newBuilder()
                .setResponseCode(ShardMasterResponse.ResponseCode.OK)
                .addAllAssignedShards(assignedShards)
                .build();
    }
}
