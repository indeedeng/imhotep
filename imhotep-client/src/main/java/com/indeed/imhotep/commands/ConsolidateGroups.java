package com.indeed.imhotep.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.Operator;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@ToString
public class ConsolidateGroups extends VoidAbstractImhotepCommand {
    private final List<String> inputGroups;
    private final Operator operation;
    private final String outputGroups;

    public ConsolidateGroups(final List<String> inputGroups, final Operator operation, final String outputGroups, final String sessionId) {
        super(sessionId, null);
        this.inputGroups = inputGroups;
        this.operation = operation;
        this.outputGroups = outputGroups;
    }

    @Override
    public void applyVoid(final ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
        imhotepSession.consolidateGroups(inputGroups, operation, outputGroups);
    }

    @Override
    protected RequestTools.ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.CONSOLIDATE_GROUPS)
                .addAllConsolidatedGroups(inputGroups)
                .setGroupConsolidationOperation(operation)
                .setOutputGroups(outputGroups)
                .setSessionId(sessionId)
                .build();
        return RequestTools.ImhotepRequestSender.Cached.create(request);
    }

    @Override
    public List<String> getInputGroups() {
        return inputGroups;
    }

    @Override
    public List<String> getOutputGroup() {
        return Arrays.asList(outputGroups);
    }
}
