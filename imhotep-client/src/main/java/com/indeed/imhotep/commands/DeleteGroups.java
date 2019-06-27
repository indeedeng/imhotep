package com.indeed.imhotep.commands;

import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@ToString
public class DeleteGroups extends VoidAbstractImhotepCommand {
    private final List<String> groupsToDelete;

    public DeleteGroups(final List<String> groupsToDelete, final String sessionId) {
        super(sessionId, null);
        this.groupsToDelete = groupsToDelete;
    }

    @Override
    public void applyVoid(final ImhotepSession imhotepSession) {
        imhotepSession.deleteGroups(groupsToDelete);
    }

    @Override
    protected RequestTools.ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.DELETE_GROUPS)
                .addAllGroupsToDelete(groupsToDelete)
                .setSessionId(sessionId)
                .build();
        return RequestTools.ImhotepRequestSender.Cached.create(request);
    }

    @Override
    public List<String> getInputGroups() {
        return groupsToDelete;
    }

    @Override
    public List<String> getOutputGroups() {
        return groupsToDelete;
    }
}
