package com.indeed.imhotep.commands;


import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode
@ToString
public class StringOrRegroup extends VoidAbstractImhotepCommand {
    private final String field;
    private final List<String> terms;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;

    public StringOrRegroup(final String field, final List<String> terms, final int targetGroup, final int negativeGroup, final int positiveGroup, final String sessionId) {
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
                .setRequestType(ImhotepRequest.RequestType.STRING_OR_REGROUP)
                .setSessionId(getSessionId())
                .setField(field)
                .addAllStringTerm(terms)
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        return ImhotepRequestSender.Cached.create(imhotepRequest);
    }


    @Override
    public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        session.stringOrRegroup(field, terms.toArray(new String[0]), targetGroup, negativeGroup, positiveGroup);
        return null;
    }
}
