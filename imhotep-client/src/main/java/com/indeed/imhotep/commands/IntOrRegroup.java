package com.indeed.imhotep.commands;

import com.google.common.primitives.Longs;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class IntOrRegroup implements ImhotepCommand<Void> {

    private final String field;
    private final long[] terms;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;

    public IntOrRegroup(final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) {
        this.field = field;
        this.terms = terms;
        this.targetGroup = targetGroup;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
    }

    @Override
    public Void combine(final List<Void> subResults) {
        return null;
    }

    @Override
    public void writeToOutputStream(final OutputStream os, final ImhotepRemoteSession imhotepRemoteSession) throws IOException {
        final ImhotepRequest imhotepRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.INT_OR_REGROUP)
                .setSessionId(imhotepRemoteSession.getSessionId())
                .setField(field)
                .addAllIntTerm(Longs.asList(terms))
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        final ImhotepRequestSender imhotepRequestSender = new ImhotepRequestSender.Simple(imhotepRequest);
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
