package com.indeed.imhotep.commands;

import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.CommandSerializationParameters;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MultiRegroupIterator extends AbstractImhotepCommand<Integer> {

    private final RegroupParams regroupParams;
    private final int numRawRules;
    private final Iterator<GroupMultiRemapRule> rules;
    @Getter(lazy = true)
    private final GroupMultiRemapRuleSender groupMultiRemapRuleSender = groupMultiRemapRuleSenderInitializer();
    private final boolean errorOnCollision;

    public MultiRegroupIterator(final RegroupParams regroupParams, final int numRawRules, final Iterator<GroupMultiRemapRule> rules, final boolean errorOnCollision, final String sessionId) {
        super(sessionId, Integer.class);
        this.regroupParams = regroupParams;
        this.numRawRules = numRawRules;
        this.rules = rules;
        this.errorOnCollision = errorOnCollision;
    }

    public GroupMultiRemapRuleSender groupMultiRemapRuleSenderInitializer() {
        return GroupMultiRemapRuleSender.createFromRules(rules, true);
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest header = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP)
                .setInputGroups(regroupParams.getInputGroups())
                .setOutputGroups(regroupParams.getOutputGroups())
                .setSessionId(getSessionId())
                .setLength(numRawRules)
                .setErrorOnCollisions(errorOnCollision)
                .build();

        return ImhotepRequestSender.Cached.create(header);
    }

    @Override
    public Integer combine(final List<Integer> subResults) {
        return Collections.max(subResults);
    }

    @Override
    public void writeToOutputStream(final OutputStream os) throws IOException {
        getImhotepRequestSender().writeToStreamNoFlush(os);
        getGroupMultiRemapRuleSender().writeToStreamNoFlush(os);
        os.flush();
    }

    @Override
    public Integer readResponse(final InputStream is, final CommandSerializationParameters serializationParameters) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse imhotepResponse = ImhotepRemoteSession.readResponseWithMemoryExceptionSessionId(is, serializationParameters.getHost(), serializationParameters.getPort(), getSessionId());
        return imhotepResponse.getNumGroups();
    }

    @Override
    public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        throw new UnsupportedOperationException("This command is only for client side and shouldn't be deserialized on the server side.");
    }
}
