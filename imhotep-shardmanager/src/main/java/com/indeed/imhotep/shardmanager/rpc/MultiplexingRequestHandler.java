package com.indeed.imhotep.shardmanager.rpc;

import com.indeed.imhotep.shardmanager.ShardManager;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerRequest;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerResponse;

/**
 * @author kenh
 */

public class MultiplexingRequestHandler implements RequestHandler {
    private final ShardAssignmentRequestHandler assignmentRequestHandler;

    public MultiplexingRequestHandler(final ShardManager shardManager) {
        this.assignmentRequestHandler = new ShardAssignmentRequestHandler(shardManager);
    }

    @Override
    public ShardManagerResponse handleRequest(final ShardManagerRequest request) {
        switch (request.getRequestType()) {
            case GET_ASSIGNMENT:
                return assignmentRequestHandler.handleRequest(request);
        }

        return ShardManagerResponse.newBuilder()
                .setResponseCode(ShardManagerResponse.ResponseCode.ERROR)
                .setErrorMessage("Unhandled request type " + request.getRequestType())
                .build();
    }
}
