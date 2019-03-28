package com.indeed.imhotep.commands;

import com.indeed.imhotep.CommandSerializationUtil;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.DocStat;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

@EqualsAndHashCode
@ToString
public class MetricRegroup extends AbstractImhotepCommand<Integer> {

    private final List<String> stats;
    private final long min;
    private final long max;
    private final long intervalSize;
    private final boolean noGutters;

    private MetricRegroup(final List<String> stats, final long min, final long max, final long intervalSize, final boolean noGutters, final String sessionId) {
        super(sessionId);
        this.stats = stats;
        this.min = min;
        this.max = max;
        this.intervalSize = intervalSize;
        this.noGutters = noGutters;
    }

    public static MetricRegroup createMetricRegroup(final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters, final String sessionId) {
        return new MetricRegroup(stat, min, max, intervalSize, noGutters, sessionId);
    }

    public static MetricRegroup createMetricRegroup(final List<String> stat, final long min, final long max, final long intervalSize, final String sessionId) {
        return new MetricRegroup(stat, min, max, intervalSize, false, sessionId);
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder().setRequestType(ImhotepRequest.RequestType.METRIC_REGROUP)
                .setSessionId(getSessionId())
                .setXStatDocstat(DocStat.newBuilder().addAllStat(stats))
                .setXMin(min)
                .setXMax(max)
                .setXIntervalSize(intervalSize)
                .setNoGutters(noGutters)
                .build();

        return ImhotepRequestSender.Cached.create(request);
    }

    @Override
    public Integer combine(final List<Integer> subResults) {
        return Collections.max(subResults);
    }

    @Override
    public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        return session.metricRegroup(stats, min, max, intervalSize, noGutters);
    }

    @Override
    public Integer readResponse(final InputStream is, final CommandSerializationUtil serializationUtil) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse imhotepResponse = ImhotepRemoteSession.readResponseWithMemoryExceptionSessionId(is, serializationUtil.getHost(), serializationUtil.getPort(), getSessionId());
        return imhotepResponse.getNumGroups();
    }

    @Override
    public Class<Integer> getResultClass() {
        return Integer.class;
    }
}
