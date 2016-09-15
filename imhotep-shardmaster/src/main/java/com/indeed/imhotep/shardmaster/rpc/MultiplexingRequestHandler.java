package com.indeed.imhotep.shardmaster.rpc;

import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;

import java.util.Collections;

/**
 * @author kenh
 */

public class MultiplexingRequestHandler implements RequestHandler {
    private final ShardAssignmentRequestHandler assignmentRequestHandler;

    public MultiplexingRequestHandler(final ShardMaster shardMaster, final int shardsResponseBatchSize) {
        assignmentRequestHandler = new ShardAssignmentRequestHandler(shardMaster, shardsResponseBatchSize);
    }

    @Override
    public Iterable<ShardMasterResponse> handleRequest(final ShardMasterRequest request) {
        switch (request.getRequestType()) {
            case GET_ASSIGNMENT:
                return assignmentRequestHandler.handleRequest(request);
            //noinspection UnnecessaryDefault
            default:
                return Collections.singletonList(ShardMasterResponse.newBuilder()
                        .setResponseCode(ShardMasterResponse.ResponseCode.ERROR)
                        .setErrorMessage("Unhandled request type " + request.getRequestType())
                        .build());
        }
    }
}
