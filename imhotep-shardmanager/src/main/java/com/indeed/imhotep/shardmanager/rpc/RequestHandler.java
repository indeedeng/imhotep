package com.indeed.imhotep.shardmanager.rpc;

import com.indeed.imhotep.shardmanager.protobuf.ShardManagerRequest;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerResponse;

/**
 * @author kenh
 */

public interface RequestHandler {
    ShardManagerResponse handleRequest(ShardManagerRequest request);
}
