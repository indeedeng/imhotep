package com.indeed.imhotep.shardmaster.rpc;

import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;

/**
 * @author kenh
 */

interface RequestHandler {
    Iterable<ShardMasterResponse> handleRequest(ShardMasterRequest request);
}
