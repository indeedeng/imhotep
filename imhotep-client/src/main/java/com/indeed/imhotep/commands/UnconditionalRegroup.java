package com.indeed.imhotep.commands;

import com.google.common.primitives.Ints;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.CommandSerializationParameters;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

@EqualsAndHashCode
@ToString
public class UnconditionalRegroup extends AbstractImhotepCommand<Integer> {

    private final int[] fromGroups;
    private final int[] toGroups;
    private final boolean filterOutNotTargeted;

    public UnconditionalRegroup(final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted, final String sessionId) {
        super(sessionId, Integer.class);
        this.fromGroups = fromGroups;
        this.toGroups = toGroups;
        this.filterOutNotTargeted = filterOutNotTargeted;
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.REMAP_GROUPS)
                .setSessionId(getSessionId())
                .addAllFromGroups(Ints.asList(fromGroups))
                .addAllToGroups(Ints.asList(toGroups))
                .setFilterOutNotTargeted(filterOutNotTargeted)
                .build();

        return ImhotepRequestSender.Cached.create(request);
    }

    @Override
    public Integer combine(final List<Integer> subResults) {
        return Collections.max(subResults);
    }

    @Override
    public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        return session.regroup(fromGroups, toGroups, filterOutNotTargeted);
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
