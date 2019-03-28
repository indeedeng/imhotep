package com.indeed.imhotep.commands;

import com.indeed.imhotep.CommandSerializationUtil;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.GroupRemapRuleArray;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
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

@EqualsAndHashCode
@ToString
public class Regroup extends AbstractImhotepCommand<Integer> {

    private final GroupRemapRule[] rules;

    private Regroup(final GroupRemapRule[] rules, final String sessionId) {
        super(sessionId);
        this.rules = rules;
    }

    public static Regroup createRegroup(final GroupRemapRule[] remapRules, final String sessionId) {
        return new Regroup(remapRules, sessionId);
    }

    public static Regroup createRegroup(final int numRawRules, final Iterator<GroupRemapRule> iterator, final String sessionId) {
        return new Regroup(new GroupRemapRuleArray(numRawRules, iterator).elements(), sessionId);
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest regroupRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.REGROUP)
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
        session.regroup(rules);
        return null;
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
