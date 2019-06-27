package com.indeed.imhotep.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@ToString
public class ResetGroups extends VoidAbstractImhotepCommand {
    private final String groupsName;

    public ResetGroups(final String groupsName, final String sessionId) {
        super(sessionId, null);
        this.groupsName = groupsName;
    }

    @Override
    public void applyVoid(final ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
        imhotepSession.resetGroups(groupsName);
    }

    @Override
    protected RequestTools.ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.RESET_GROUPS)
                .setInputGroups(groupsName)
                .setSessionId(sessionId)
                .build();
        return RequestTools.ImhotepRequestSender.Cached.create(request);
    }

    @Override
    public List<String> getInputGroups() {
        return Arrays.asList(groupsName);
    }

    @Override
    public List<String> getOutputGroups() {
        return Arrays.asList(groupsName);
    }
}
