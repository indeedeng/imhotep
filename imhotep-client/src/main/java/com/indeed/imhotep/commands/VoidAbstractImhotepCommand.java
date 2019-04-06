package com.indeed.imhotep.commands;

import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.CommandSerializationParameters;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public abstract class VoidAbstractImhotepCommand extends AbstractImhotepCommand<Void> {

    VoidAbstractImhotepCommand(final String sessionId) {
        super(sessionId, Void.class);
    }

    @Override
    public Void combine(final List<Void> subResults) {
        return null;
    }

    @Override
    public Void apply(ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
        applyVoid(imhotepSession);
        return null;
    }

    public abstract void applyVoid(ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException;

    @Override
    public Void readResponse(final InputStream is, final CommandSerializationParameters serializationParameters) throws IOException, ImhotepOutOfMemoryException {
        ImhotepRemoteSession.readResponseWithMemoryExceptionSessionId(is, serializationParameters.getHost(), serializationParameters.getPort(), getSessionId());
        return null;
    }

    @Override
    public Class<Void> getResultClass() {
        return Void.class;
    }
}
