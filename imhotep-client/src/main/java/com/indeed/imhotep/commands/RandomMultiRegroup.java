package com.indeed.imhotep.commands;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class RandomMultiRegroup implements ImhotepCommand<Void> {

    private final String field;
    private final boolean isIntField;
    private final String salt;
    private final int targetGroup;
    private final double[] percentages;
    private final int[] resultGroups;

    public RandomMultiRegroup(final String field, final boolean isIntField, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) {
        this.field =field;
        this.isIntField = isIntField;
        this.salt = salt;
        this.targetGroup = targetGroup;
        this.percentages = percentages;
        this.resultGroups = resultGroups;
    }

    @Override
    public Void combine(final List<Void> subResults) {
        return null;
    }

    @Override
    public void writeToOutputStream(final OutputStream os, final ImhotepRemoteSession imhotepRemoteSession) throws IOException {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.RANDOM_MULTI_REGROUP)
                .setSessionId(imhotepRemoteSession.getSessionId())
                .setField(field)
                .setIsIntField(isIntField)
                .setSalt(salt)
                .setTargetGroup(targetGroup)
                .addAllPercentages(Doubles.asList(percentages))
                .addAllResultGroups(Ints.asList(resultGroups))
                .build();

        final ImhotepRequestSender imhotepRequestSender = new ImhotepRequestSender.Simple(request);
        imhotepRemoteSession.sendRequestReadNoResponseFlush(imhotepRequestSender, os);
    }

    @Override
    public Void readResponse(final InputStream is, final ImhotepRemoteSession imhotepRemoteSession) throws IOException, ImhotepOutOfMemoryException {
        imhotepRemoteSession.readResponseWithMemoryExceptionFromInputStream(is);
        return null;
    }

    @Override
    public Class<Void> getResultClass() {
        return Void.class;
    }
}
