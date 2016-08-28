package com.indeed.imhotep.shardmanager.rpc;

import com.indeed.imhotep.shardmanager.ShardManager;
import com.indeed.imhotep.shardmanager.protobuf.AssignedShard;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerRequest;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerResponse;

import java.io.IOException;

/**
 * @author kenh
 */

class ShardAssignmentRequestHandler implements RequestHandler {
    private final ShardManager shardManager;

    ShardAssignmentRequestHandler(final ShardManager shardManager) {
        this.shardManager = shardManager;
    }

    @Override
    public ShardManagerResponse handleRequest(final ShardManagerRequest request) {
        final String node = request.getNode().getHost();
        final Iterable<AssignedShard> assignedShards;
        try {
            assignedShards = shardManager.getAssignments(node);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get shards for " + node, e);
        }

        return ShardManagerResponse.newBuilder()
                .setResponseCode(ShardManagerResponse.ResponseCode.OK)
                .addAllAssignedShards(assignedShards)
                .build();
    }
}
