package com.indeed.imhotep.commands;

import com.google.common.primitives.Longs;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class IntOrRegroup extends VoidAbstractImhotepCommand {

    private final String field;
    private final long[] terms;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;

    public IntOrRegroup(final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup, final String sessionId) {
        super(sessionId);
        this.field = field;
        this.terms = terms;
        this.targetGroup = targetGroup;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest imhotepRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.INT_OR_REGROUP)
                .setSessionId(getSessionId())
                .setField(field)
                .addAllIntTerm(Longs.asList(terms))
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        return ImhotepRequestSender.Cached.create(imhotepRequest);
    }

    @Override
    public void applyVoid(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
    }
}
