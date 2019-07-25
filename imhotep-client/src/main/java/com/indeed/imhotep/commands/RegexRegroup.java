package com.indeed.imhotep.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString
public class RegexRegroup extends VoidAbstractImhotepCommand {

    private final RegroupParams regroupParams;
    private final String field;
    private final String regex;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;

    public RegexRegroup(final RegroupParams regroupParams, final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup, final String sessionId) {
        super(sessionId, regroupParams);
        this.regroupParams = regroupParams;
        this.field = field;
        this.regex = regex;
        this.targetGroup = targetGroup;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
    }

    @Override
    protected ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.REGEX_REGROUP)
                .setInputGroups(regroupParams.getInputGroups())
                .setOutputGroups(regroupParams.getOutputGroups())
                .setSessionId(getSessionId())
                .setField(field)
                .setRegex(regex)
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        return ImhotepRequestSender.Cached.create(request);
    }

    @Override
    public void applyVoid(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        session.regexRegroup(regroupParams, field, regex, targetGroup, negativeGroup, positiveGroup);
    }
}
