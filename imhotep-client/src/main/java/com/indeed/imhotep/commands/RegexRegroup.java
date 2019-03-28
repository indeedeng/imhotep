package com.indeed.imhotep.commands;


import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class RegexRegroup extends VoidAbstractImhotepCommand {

    private final String field;
    private final String regex;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;

    public RegexRegroup(final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup, final String sessionId) {
        super(sessionId);
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
    public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup);
        return null;
    }
}
