package com.indeed.imhotep.commands;

import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MultiRegroup implements ImhotepCommand<Integer> {

    private final GroupMultiRemapRuleSender groupMultiRemapRuleSender;
    @Nullable
    private final Boolean errorOnCollision;

    private MultiRegroup(final GroupMultiRemapRuleSender groupMultiRemapRuleSender, final Boolean errorOnCollision) {
        this.groupMultiRemapRuleSender = groupMultiRemapRuleSender;
        this.errorOnCollision = errorOnCollision;
    }

    public static MultiRegroup creatMultiRegroupCommand(final GroupMultiRemapRule[] rules, final Boolean errorOnCollision) {
        final GroupMultiRemapRuleSender groupMultiRemapRuleSender = GroupMultiRemapRuleSender.createFromRules(Arrays.asList(rules).iterator(), false);
        return new MultiRegroup(groupMultiRemapRuleSender, errorOnCollision);
    }

    public static MultiRegroup createMultiRegroupCommand(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules, final Boolean errorOnCollision) {
        final GroupMultiRemapRuleSender groupMultiRemapRuleSender = GroupMultiRemapRuleSender.createFromRules(rawRules, false);
        return new MultiRegroup(groupMultiRemapRuleSender, errorOnCollision);
    }

    public static MultiRegroup createMultiRegreoupCommand(final GroupMultiRemapMessage[] rawRuleMessages, boolean errorOnCollisions) {
        final GroupMultiRemapRuleSender groupMultiRemapRuleSender = GroupMultiRemapRuleSender.createFromMessages(Arrays.asList(rawRuleMessages).iterator(), errorOnCollisions);
        return new MultiRegroup(groupMultiRemapRuleSender, errorOnCollisions);
    }

    public static MultiRegroup createMultiRegroupCommand(final GroupMultiRemapRuleSender groupMultiRemapRuleSender, final boolean errorOnCollision) {
        return new MultiRegroup(groupMultiRemapRuleSender, errorOnCollision);
    }

    @Override
    public Integer combine(final List<Integer> subResults) {
        return Collections.max(subResults);
    }

    @Override
    public void writeToOutputStream(final OutputStream os, final ImhotepRemoteSession imhotepRemoteSession ) throws IOException {
        final ImhotepRequest.Builder requestBuilder = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP)
                .setSessionId(imhotepRemoteSession.getSessionId())
                .setLength(groupMultiRemapRuleSender.getRulesCount());
        if (errorOnCollision) {
            requestBuilder.setErrorOnCollisions(errorOnCollision);
        }
        final ImhotepRequest header = requestBuilder.build();

        final ImhotepRequestSender imhotepRequestSender = new ImhotepRequestSender.Simple(header);
        imhotepRemoteSession.sendRequestReadNoResponseFlush(imhotepRequestSender, os);

        groupMultiRemapRuleSender.writeToStreamNoFlush(os);
        os.flush();
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
