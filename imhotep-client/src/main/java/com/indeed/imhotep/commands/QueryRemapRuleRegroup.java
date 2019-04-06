package com.indeed.imhotep.commands;

import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.api.CommandSerializationParameters;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
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
public class QueryRemapRuleRegroup extends AbstractImhotepCommand<Integer> {

    final QueryRemapRule rule;

    public QueryRemapRuleRegroup(final QueryRemapRule rule, final String sessionId) {
        super(sessionId, Integer.class);
        this.rule = rule;
    }

    @Override
    protected RequestTools.ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.QUERY_REGROUP)
                .setSessionId(getSessionId())
                .setQueryRemapRule(ImhotepClientMarshaller.marshal(rule))
                .build();

        return RequestTools.ImhotepRequestSender.Cached.create(request);
    }

    @Override
    public Integer combine(final List<Integer> subResults) {
        return Collections.max(subResults);
    }

    @Override
    public Integer readResponse(final InputStream is, final CommandSerializationParameters serializationParameters) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse imhotepResponse = ImhotepRemoteSession.readResponseWithMemoryExceptionSessionId(is, serializationParameters.getHost(), serializationParameters.getPort(), getSessionId());
        return imhotepResponse.getNumGroups();
    }

    @Override
    public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        return session.regroup(rule);
    }
}
