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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@EqualsAndHashCode
@ToString
/**
 * Initializes the groupMultiRemapRuleSender only when its getter is called, to avoid its creation on the server side.
 */
public class MultiRegroup extends AbstractImhotepCommand<Integer> {

    private final RegroupParams regroupParams;
    private final GroupMultiRemapRule[] rules;
    @Getter(lazy = true)
    private final GroupMultiRemapRuleSender groupMultiRemapRuleSender = GroupMultiRemapRuleSender.createFromRules(Arrays.asList(rules).iterator(), true);
    private final boolean errorOnCollision;

    private MultiRegroup(final RegroupParams regroupParams, final GroupMultiRemapRule[] rules, final boolean errorOnCollision, final String sessionId) {
        super(sessionId, Integer.class);
        this.regroupParams = regroupParams;
        this.rules = rules;
        this.errorOnCollision = errorOnCollision;
    }

    public static MultiRegroup createMultiRegroupCommand(final RegroupParams regroupParams, final GroupMultiRemapRule[] rules, final boolean errorOnCollision, final String sessionId) {
        return new MultiRegroup(regroupParams, rules, errorOnCollision, sessionId);
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest header = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP)
                .setInputGroups(regroupParams.getInputGroups())
                .setOutputGroups(regroupParams.getOutputGroups())
                .setSessionId(getSessionId())
                .setLength(rules.length)
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

    public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        return session.regroup(regroupParams, rules, errorOnCollision);
    }

    @Override
    public Integer readResponse(final InputStream is, final CommandSerializationParameters serializationParameters) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse imhotepResponse = ImhotepRemoteSession.readResponseWithMemoryExceptionSessionId(is, serializationParameters.getHost(), serializationParameters.getPort(), getSessionId());
        return imhotepResponse.getNumGroups();
    }
}
