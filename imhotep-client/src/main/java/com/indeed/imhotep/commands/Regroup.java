package com.indeed.imhotep.commands;

import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.GroupRemapRuleArray;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.CommandSerializationParameters;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@ToString
public class Regroup extends AbstractImhotepCommand<Integer> {

    private final RegroupParams regroupParams;
    private final GroupRemapRule[] rules;

    private Regroup(final RegroupParams regroupParams, final GroupRemapRule[] rules, final String sessionId) {
        super(sessionId, Integer.class);
        this.regroupParams = regroupParams;
        this.rules = rules;
    }

    public static Regroup createRegroup(final RegroupParams regroupParams, final GroupRemapRule[] remapRules, final String sessionId) {
        return new Regroup(regroupParams, remapRules, sessionId);
    }

    public static Regroup createRegroup(final RegroupParams regroupParams, final int numRawRules, final Iterator<GroupRemapRule> iterator, final String sessionId) {
        return new Regroup(regroupParams, new GroupRemapRuleArray(numRawRules, iterator).elements(), sessionId);
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest regroupRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.REGROUP)
                .setInputGroups(regroupParams.getInputGroups())
                .setOutputGroups(regroupParams.getOutputGroups())
                .setSessionId(getSessionId())
                .setLength(rules.length)
                .addAllRemapRules(ImhotepClientMarshaller.marshal(rules))
                .build();

        return ImhotepRequestSender.Cached.create(regroupRequest);
    }

    @Override
    public Integer combine(final List<Integer> subResults) {
        return Collections.max(subResults);
    }

    @Override
    public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        session.regroup(regroupParams, rules);
        return null;
    }

    @Override
    public Integer readResponse(final InputStream is, final CommandSerializationParameters serializationParameters) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse imhotepResponse = ImhotepRemoteSession.readResponseWithMemoryExceptionSessionId(is, serializationParameters.getHost(), serializationParameters.getPort(), getSessionId());
        return imhotepResponse.getNumGroups();
    }

    @Override
    public Class<Integer> getResultClass() {
        return Integer.class;
    }
}
