package com.indeed.imhotep.commands;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.DocStat;
import com.indeed.imhotep.protobuf.ImhotepRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class RandomMetricMultiRegroup implements ImhotepCommand<Integer> {

    private final List<String> stat;
    private final String salt;
    private final int targetGroup;
    private final double[] percentages;
    private final int[] resultGroups;

    public RandomMetricMultiRegroup(List<String> stat, String salt, int targetGroup, double[] percentages, int[] resultGroups) {
        this.stat = stat;
        this.salt = salt;
        this.targetGroup = targetGroup;
        this.percentages = percentages;
        this.resultGroups = resultGroups;
    }

    @Override
    public Integer combine(final Integer... subResults) {
        return null;
    }

    @Override
    public void writeToOutputStream(final OutputStream os, final ImhotepRemoteSession imhotepRemoteSession) throws IOException {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.RANDOM_METRIC_MULTI_REGROUP)
                .setSessionId(imhotepRemoteSession.getSessionId())
                .addDocStat(DocStat.newBuilder().addAllStat(stat))
                .setHasStats(true)
                .setSalt(salt)
                .setTargetGroup(targetGroup)
                .addAllPercentages(Doubles.asList(percentages))
                .addAllResultGroups(Ints.asList(resultGroups))
                .build();

        final ImhotepRequestSender imhotepRequestSender = new ImhotepRequestSender.Simple(request);
        imhotepRemoteSession.sendRequestReadNoResponseFlush(imhotepRequestSender, os);
    }

    @Override
    public Integer readResponse(final InputStream is, final ImhotepRemoteSession imhotepRemoteSession) throws IOException, ImhotepOutOfMemoryException {
        imhotepRemoteSession.readResponseWithMemoryExceptionFromInputStream(is);
        return null;
    }

    @Override
    public Integer[] getExecutionBuffer(int length) {
        return new Integer[length];
    }
}
