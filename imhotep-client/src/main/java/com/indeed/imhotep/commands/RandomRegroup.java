package com.indeed.imhotep.commands;

import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RandomRegroup implements ImhotepCommand<Integer> {

    private final String field;
    private final boolean isIntField;
    private final String salt;
    private final double p;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;

    public RandomRegroup(final String field, final boolean isIntField, final String salt, final double p, final int targetGroup,
                         final int negativeGroup, final int positiveGroup) {
        this.field = field;
        this.isIntField = isIntField;
        this.salt = salt;
        this.p = p;
        this.targetGroup = targetGroup;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
    }
    @Override
    public Integer combine(final Integer... subResults) {
        return null;
    }

    @Override
    public void writeToOutputStream(final OutputStream os, final ImhotepRemoteSession imhotepRemoteSession) throws IOException {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.RANDOM_REGROUP)
                .setSessionId(imhotepRemoteSession.getSessionId())
                .setField(field)
                .setIsIntField(isIntField)
                .setSalt(salt)
                .setP(p)
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
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
