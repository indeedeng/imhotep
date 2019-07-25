package com.indeed.imhotep.commands;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.DocStat;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@ToString
public class RandomMetricMultiRegroup extends VoidAbstractImhotepCommand {

    private final RegroupParams regroupParams;
    private final List<String> stat;
    private final String salt;
    private final int targetGroup;
    private final double[] percentages;
    private final int[] resultGroups;

    public RandomMetricMultiRegroup(final RegroupParams regroupParams, final List<String> stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups, final String sessionId) {
        super(sessionId, regroupParams);
        this.regroupParams = regroupParams;
        this.stat = stat;
        this.salt = salt;
        this.targetGroup = targetGroup;
        this.percentages = percentages;
        this.resultGroups = resultGroups;
    }

    @Override
    public ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.RANDOM_METRIC_MULTI_REGROUP)
                .setInputGroups(regroupParams.getInputGroups())
                .setOutputGroups(regroupParams.getOutputGroups())
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
    public void applyVoid(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        session.randomMetricMultiRegroup(regroupParams, stat, salt, targetGroup, percentages, resultGroups);
    }
}
