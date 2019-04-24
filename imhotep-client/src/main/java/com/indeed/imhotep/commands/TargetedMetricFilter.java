package com.indeed.imhotep.commands;

import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.CommandSerializationParameters;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.DocStat;
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
public class TargetedMetricFilter extends AbstractImhotepCommand<Integer> {

    private final List<String> stat;
    private final long min;
    private final long max;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;

    public TargetedMetricFilter(final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup, final String sessionId) {
        super(sessionId, Integer.class);
        this.stat = stat;
        this.min = min;
        this.max = max;
        this.targetGroup = targetGroup;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.METRIC_FILTER)
                .setSessionId(getSessionId())
                .setXStatDocstat(DocStat.newBuilder().addAllStat(stat))
                .setXMin(min)
                .setXMax(max)
                .setTargetGroup(targetGroup)
                .setPositiveGroup(positiveGroup)
                .setNegativeGroup(negativeGroup)
                .build();

        return ImhotepRequestSender.Cached.create(request);
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
        return session.metricFilter(stat, min, max, targetGroup, negativeGroup, positiveGroup);
    }
}