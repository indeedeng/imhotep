package com.indeed.imhotep.shardmaster.rpc;

import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;

/**
 * @author kenh
 */

public class MultiplexingRequestHandler implements RequestHandler {
    private final ShardAssignmentRequestHandler assignmentRequestHandler;

    public MultiplexingRequestHandler(final ShardMaster shardMaster) {
        this.assignmentRequestHandler = new ShardAssignmentRequestHandler(shardMaster);
    }

    @Override
    public ShardMasterResponse handleRequest(final ShardMasterRequest request) {
        switch (request.getRequestType()) {
            case GET_ASSIGNMENT:
                return assignmentRequestHandler.handleRequest(request);
            default:
                return ShardMasterResponse.newBuilder()
                        .setResponseCode(ShardMasterResponse.ResponseCode.ERROR)
                        .setErrorMessage("Unhandled request type " + request.getRequestType())
                        .build();
        }
    }
}
