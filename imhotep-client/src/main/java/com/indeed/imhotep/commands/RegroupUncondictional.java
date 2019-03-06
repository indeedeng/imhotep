package com.indeed.imhotep.commands;

import com.google.common.primitives.Ints;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;

public class RegroupUncondictional implements ImhotepCommand<Integer> {

    private final int[] fromGroups;
    private final int[] toGroups;
    private final boolean filterOutNotTargeted;

    public RegroupUncondictional(final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) {
        this.fromGroups = fromGroups;
        this.toGroups = toGroups;
        this.filterOutNotTargeted = filterOutNotTargeted;
    }

    @Override
    public Integer combine(final Integer... subResults) {
        return Collections.max(Arrays.asList(subResults));
    }

    @Override
    public void writeToOutputStream(final OutputStream os, final ImhotepRemoteSession imhotepRemoteSession) throws IOException {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.REMAP_GROUPS)
                .setSessionId(imhotepRemoteSession.getSessionId())
                .addAllFromGroups(Ints.asList(fromGroups))
                .addAllToGroups(Ints.asList(toGroups))
                .setFilterOutNotTargeted(filterOutNotTargeted)
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
    public Integer[] getExecutionBuffer(final int length) {
        return new Integer[length];
    }
}
