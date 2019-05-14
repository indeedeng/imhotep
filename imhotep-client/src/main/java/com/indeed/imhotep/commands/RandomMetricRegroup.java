package com.indeed.imhotep.commands;

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
public class RandomMetricRegroup extends VoidAbstractImhotepCommand {

    private final RegroupParams regroupParams;
    private final List<String> stat;
    private final String salt;
    private final double p;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;

    public RandomMetricRegroup(final RegroupParams regroupParams, final List<String> stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup, final String sessionId) {
        super(sessionId);
        this.regroupParams = regroupParams;
        this.stat = stat;
        this.salt = salt;
        this.p = p;
        this.targetGroup = targetGroup;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.RANDOM_METRIC_REGROUP)
                .setInputGroups(regroupParams.getInputGroups())
                .setOutputGroups(regroupParams.getOutputGroups())
                .setSessionId(sessionId)
                .addDocStat(DocStat.newBuilder().addAllStat(stat))
                .setHasStats(true)
                .setSalt(salt)
                .setP(p)
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        return ImhotepRequestSender.Cached.create(request);
    }

    @Override
    public void applyVoid(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        session.randomMetricRegroup(regroupParams, stat, salt, p, targetGroup, negativeGroup, positiveGroup);
    }
}
