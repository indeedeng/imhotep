package com.indeed.imhotep;

import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.scheduling.SchedulerType;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Holds the context of a request being processed by the server
 */
public class RequestContext {
    public static final ThreadLocal<RequestContext> THREAD_REQUEST_CONTEXT = new ThreadLocal<>();

    @Nullable
    private final String sessionId;
    private final ImhotepRequest.RequestType requestType;

    private final SlotTiming totalRpcTime = new SlotTiming();

    public RequestContext(final ImhotepRequest request) {
        this.sessionId = request.hasSessionId() ? request.getSessionId() : null;
        this.requestType = request.getRequestType();
    }

    public ImhotepRequest.RequestType getRequestType() {
        return requestType;
    }

    @Nullable
    public String getSessionId() {
        return sessionId;
    }

    public SlotTiming getTotalRpcTime() {
        return totalRpcTime;
    }

    public void schedulerExecTimeCallback(final SchedulerType schedulerType, final long execTime) {
        totalRpcTime.schedulerExecTimeCallback(schedulerType, execTime);
    }

    public void schedulerWaitTimeCallback(final SchedulerType schedulerType, final long waitTime) {
        totalRpcTime.schedulerWaitTimeCallback(schedulerType, waitTime);
    }

    public void addTimingProperties(final Map<String, Object> properties) {
        totalRpcTime.addTimingProperties(properties);
    }
}
