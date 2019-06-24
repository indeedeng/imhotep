package com.indeed.imhotep.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@ToString
public class StringOrRegroup extends VoidAbstractImhotepCommand {
    private final RegroupParams regroupParams;
    private final String field;
    private final List<String> terms;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;

    public StringOrRegroup(final RegroupParams regroupParams, final String field, final List<String> terms, final int targetGroup, final int negativeGroup, final int positiveGroup, final String sessionId) {
        super(sessionId, regroupParams);
        this.regroupParams = regroupParams;
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
                .setInputGroups(regroupParams.getInputGroups())
                .setOutputGroups(regroupParams.getOutputGroups())
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
    public void applyVoid(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        session.stringOrRegroup(regroupParams, field, terms.toArray(new String[0]), targetGroup, negativeGroup, positiveGroup);
    }
}
