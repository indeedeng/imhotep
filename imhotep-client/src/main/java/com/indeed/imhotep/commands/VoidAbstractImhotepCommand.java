package com.indeed.imhotep.commands;

import com.indeed.imhotep.CommandSerializationUtil;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public abstract class VoidAbstractImhotepCommand extends AbstractImhotepCommand<Void> {

    VoidAbstractImhotepCommand(final String sessionId) {
        super(sessionId);
    }

    @Override
    public Void combine(final List<Void> subResults) {
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
