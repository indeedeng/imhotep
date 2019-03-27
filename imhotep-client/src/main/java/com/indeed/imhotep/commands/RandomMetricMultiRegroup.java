package com.indeed.imhotep.commands;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.indeed.imhotep.CommandSerializationUtil;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.DocStat;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

@EqualsAndHashCode
@ToString
public class RandomMetricMultiRegroup implements ImhotepCommand<Void> {

    private final List<String> stat;
    private final String salt;
    private final int targetGroup;
    private final double[] percentages;
    private final int[] resultGroups;
    @Getter private final String sessionId;
    @Getter(lazy = true) private final ImhotepRequestSender imhotepRequestSender = imhotepRequestSenderInitializer();

    public RandomMetricMultiRegroup(final List<String> stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups, final String sessionId) {
        this.stat = stat;
        this.salt = salt;
        this.targetGroup = targetGroup;
        this.percentages = percentages;
        this.resultGroups = resultGroups;
        this.sessionId = sessionId;
    }

    private ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.RANDOM_METRIC_MULTI_REGROUP)
                .setSessionId(getSessionId())
                .addDocStat(DocStat.newBuilder().addAllStat(stat))
                .setHasStats(true)
                .setSalt(salt)
                .setTargetGroup(targetGroup)
                .addAllPercentages(Doubles.asList(percentages))
                .addAllResultGroups(Ints.asList(resultGroups))
                .build();

        return ImhotepRequestSender.Cached.create(request);
     }

    @Override
    public Void combine(final List<Void> subResults) {
        return null;
    }

    @Override
    public void writeToOutputStream(final OutputStream os) throws IOException {
        ImhotepRemoteSession.sendRequestReadNoResponseFlush(getImhotepRequestSender(), os);
    }

    @Override
    public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        session.randomMetricMultiRegroup(stat, salt, targetGroup, percentages, resultGroups);
        return null;
    }

    @Override
    public Void readResponse(final InputStream is, final CommandSerializationUtil serializationUtil) throws IOException, ImhotepOutOfMemoryException {
        ImhotepRemoteSession.readResponseWithMemoryExceptionSessionId(is, serializationUtil.getHost(), serializationUtil.getPort(), getSessionId());
        return null;
    }

    @Override
    public Class<Void> getResultClass() {
        return Void.class;
    }
}
