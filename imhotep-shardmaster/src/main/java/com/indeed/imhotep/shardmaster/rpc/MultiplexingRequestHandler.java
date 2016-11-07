package com.indeed.imhotep.shardmaster.rpc;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @author kenh
 */

public class MultiplexingRequestHandler implements RequestHandler {
    private final RequestMetricStatsEmitter statsEmitter;
    private final ShardAssignmentRequestHandler assignmentRequestHandler;

    public MultiplexingRequestHandler(final RequestMetricStatsEmitter statsEmitter, final ShardMaster shardMaster, final int shardsResponseBatchSize) {
        this.statsEmitter = statsEmitter;
        assignmentRequestHandler = new ShardAssignmentRequestHandler(shardMaster, shardsResponseBatchSize);
    }

    private static final String METRIC_PROCESSED = "request.processed";
    private static final String METRIC_ERROR = "request.error";

    @Override
    public Iterable<ShardMasterResponse> handleRequest(final ShardMasterRequest request) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            final Iterable<ShardMasterResponse> result;
            switch (request.getRequestType()) {
                case GET_ASSIGNMENT:
                    result = assignmentRequestHandler.handleRequest(request);
                    break;
                default:
                    result = Collections.singletonList(ShardMasterResponse.newBuilder()
                            .setResponseCode(ShardMasterResponse.ResponseCode.ERROR)
                            .setErrorMessage("Unhandled request type " + request.getRequestType())
                            .build());
                    statsEmitter.processed(METRIC_ERROR, request.getRequestType(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
                    return result;
            }
            statsEmitter.processed(METRIC_PROCESSED, request.getRequestType(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            return result;
        } catch (final Throwable e) {
            statsEmitter.processed(METRIC_ERROR, request.getRequestType(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw Throwables.propagate(e);
        }
    }
}
