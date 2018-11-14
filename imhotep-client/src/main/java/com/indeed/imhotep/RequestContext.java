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

    private final ImhotepRequest request;

    private final SlotTiming totalRpcTime = new SlotTiming();

    public RequestContext(final ImhotepRequest request) {
        this.request = request;
    }

    public ImhotepRequest.RequestType getRequestType() {
        return request.getRequestType();
    }

    @Nullable
    public String getSessionId() {
        if (request.hasSessionId()) {
            return request.getSessionId();
        }
        return null;
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
