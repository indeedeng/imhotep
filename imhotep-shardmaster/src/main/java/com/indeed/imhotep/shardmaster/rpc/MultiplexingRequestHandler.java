package com.indeed.imhotep.shardmaster.rpc;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @author kenh
 */

public class MultiplexingRequestHandler implements RequestHandler {
    private static final Logger LOGGER = Logger.getLogger(MultiplexingRequestHandler.class);
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
        LOGGER.info("Handling " + request.getRequestType() + " request from " + request.getNode());
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
                    LOGGER.warn("Not handling unknown request " + request.getRequestType() + " from " + request.getNode());
                    return result;
            }
            final long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            statsEmitter.processed(METRIC_PROCESSED, request.getRequestType(), elapsed);
            LOGGER.info("Finished handling request " + request.getRequestType() + " from " + request.getNode() + " in " + elapsed +" ms");
            return result;
        } catch (final Throwable e) {
            final long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            statsEmitter.processed(METRIC_ERROR, request.getRequestType(), elapsed);
            LOGGER.error("Error while handling request " + request.getRequestType() + " from " + request.getNode() + " in " + elapsed +" ms", e);
            throw Throwables.propagate(e);
        }
    }
}
