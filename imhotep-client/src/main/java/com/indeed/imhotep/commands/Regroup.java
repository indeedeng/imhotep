package com.indeed.imhotep.commands;

import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.GroupRemapRuleArray;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Regroup implements ImhotepCommand<Integer> {

    private final GroupRemapRule[] rules;

    private Regroup(final GroupRemapRule[] rules) {
        this.rules = rules;
    }

    public static Regroup createRegroup(final GroupRemapRule[] remapRules) {
        return new Regroup(remapRules);
    }

    public static Regroup createRegroup(final int numRawRules, final Iterator<GroupRemapRule> iterator) {
        return new Regroup(new GroupRemapRuleArray(numRawRules, iterator).elements());
    }

    @Override
    public Integer combine(final List<Integer> subResults) {
        return Collections.max(subResults);
    }

    @Override
    public void writeToOutputStream(final OutputStream os, final ImhotepRemoteSession imhotepRemoteSession) throws IOException {
        final ImhotepRequest regroupRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.REGROUP)
                .setSessionId(imhotepRemoteSession.getSessionId())
                .setLength(rules.length)
                .addAllRemapRules(ImhotepClientMarshaller.marshal(rules))
                .build();

        final ImhotepRequestSender imhotepRequestSender = new ImhotepRequestSender.Simple(regroupRequest);
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
