package com.indeed.imhotep.commands;

import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.DocStat;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MetricRegroup implements ImhotepCommand<Integer> {

    private final List<String> stats;
    private final long min;
    private final long max;
    private final long intervalSize;
    private final boolean noGutters;

    private MetricRegroup(final List<String> stats, final long min, final long max, final long intervalSize, final boolean noGutters) {
        this.stats = stats;
        this.min = min;
        this.max = max;
        this.intervalSize = intervalSize;
        this.noGutters = noGutters;
    }

    public static MetricRegroup createMetricRegroup(final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters) {
        return new MetricRegroup(stat, min, max, intervalSize, noGutters);
    }

    public static MetricRegroup createMetricRegroup(final List<String> stat, final long min, final long max, final long intervalSize) {
        return new MetricRegroup(stat, min, max, intervalSize, false);
    }

    @Override
    public Integer combine(final List<Integer> subResults) {
        return Collections.max(subResults);
    }

    @Override
    public void writeToOutputStream(final OutputStream os, final ImhotepRemoteSession imhotepRemoteSession) throws IOException {
        final ImhotepRequest request = ImhotepRequest.newBuilder().setRequestType(ImhotepRequest.RequestType.METRIC_REGROUP)
                .setSessionId(imhotepRemoteSession.getSessionId())
                .setXStatDocstat(DocStat.newBuilder().addAllStat(stats))
                .setXMin(min)
                .setXMax(max)
                .setXIntervalSize(intervalSize)
                .setNoGutters(noGutters)
                .build();

        final ImhotepRequestSender imhotepRequestSender = new ImhotepRequestSender.Simple(request);
        imhotepRemoteSession.sendRequestReadNoResponseFlush(imhotepRequestSender, os);
    }

    @Override
    public Integer readResponse(final InputStream is, final ImhotepRemoteSession imhotepRemoteSession) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse imhotepResponse = imhotepRemoteSession.readResponseWithMemoryExceptionFromInputStream(is);
        return imhotepResponse.getNumGroups();
    }

    @Override
    public Class<Integer> getResultClass() {
        return Integer.class;
    }
}
