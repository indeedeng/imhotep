package com.indeed.imhotep.commands;

import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString
public class DeleteGroups extends VoidAbstractImhotepCommand {
    private final String groupsName;

    public DeleteGroups(final String groupsName, final String sessionId) {
        super(sessionId);
        this.groupsName = groupsName;
    }

    @Override
    public void applyVoid(final ImhotepSession imhotepSession) {
        imhotepSession.deleteGroups(groupsName);
    }

    @Override
    protected RequestTools.ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.DELETE_GROUPS)
                .setInputGroups(groupsName)
                .setSessionId(sessionId)
                .build();
        return RequestTools.ImhotepRequestSender.Cached.create(request);
    }
}
