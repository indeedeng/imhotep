package com.indeed.imhotep.commands;

import com.indeed.imhotep.CommandSerializationUtil;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MultiRegroupMessagesSender extends AbstractImhotepCommand<Integer> {

    private final GroupMultiRemapRuleSender groupMultiRemapRuleSender;
    private final boolean errorOnCollision;

    private MultiRegroupMessagesSender(final GroupMultiRemapRuleSender groupMultiRemapRuleSender, final boolean errorOnCollision, final String sessionId) {
        super(sessionId);
        this.groupMultiRemapRuleSender = groupMultiRemapRuleSender;
        this.errorOnCollision = errorOnCollision;
    }

    public static MultiRegroupMessagesSender createMultiRegroupMessagesSender(final GroupMultiRemapMessage[] messages, final boolean errorOnCollision, final String sessionId) {
        return new MultiRegroupMessagesSender(GroupMultiRemapRuleSender.createFromMessages(Arrays.asList(messages).iterator(), true), errorOnCollision, sessionId);
    }

    public static MultiRegroupMessagesSender createMultiRegroupMessagesSender(final GroupMultiRemapRuleSender groupMultiRemapRuleSender, final boolean errorOnCollision, final String sessionId) {
        return new MultiRegroupMessagesSender(groupMultiRemapRuleSender, errorOnCollision, sessionId);
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest header = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP)
                .setSessionId(getSessionId())
                .setLength(groupMultiRemapRuleSender.getRulesCount())
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
        ImhotepRemoteSession.sendRequestReadNoResponseNoFlush(getImhotepRequestSender(), os);
        groupMultiRemapRuleSender.writeToStreamNoFlush(os);
        os.flush();
    }

    @Override
    public Integer apply(final ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
        throw new IllegalStateException("This command is only for client side and shouldn't be deserialized on the server side.");
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
