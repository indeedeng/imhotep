package com.indeed.imhotep.commands;

import com.indeed.imhotep.CommandSerializationUtil;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
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
public class RandomRegroup implements ImhotepCommand<Void> {

    private final String field;
    private final boolean isIntField;
    private final String salt;
    private final double p;
    private final int targetGroup;
    private final int negativeGroup;
    private final int positiveGroup;
    @Getter private final String sessionId;
    @Getter(lazy = true) private final ImhotepRequestSender imhotepRequestSender = imhotepRequestSenderInitializer();

    public RandomRegroup(final String field, final boolean isIntField, final String salt, final double p, final int targetGroup,
                         final int negativeGroup, final int positiveGroup, final String sessionId) {
        this.field = field;
        this.isIntField = isIntField;
        this.salt = salt;
        this.p = p;
        this.targetGroup = targetGroup;
        this.negativeGroup = negativeGroup;
        this.positiveGroup = positiveGroup;
        this.sessionId = sessionId;
    }

    private ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.RANDOM_REGROUP)
                .setSessionId(getSessionId())
                .setField(field)
                .setIsIntField(isIntField)
                .setSalt(salt)
                .setP(p)
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
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
        session.randomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
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
