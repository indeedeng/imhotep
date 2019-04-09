package com.indeed.imhotep.commands;

import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import lombok.Getter;

import java.io.IOException;
import java.io.OutputStream;

public abstract class AbstractImhotepCommand<T> implements ImhotepCommand<T> {

    @Getter final String sessionId;
    @Getter(lazy = true) private final ImhotepRequestSender imhotepRequestSender = imhotepRequestSenderInitializer();
    @Getter final Class<T> resultClass;

    AbstractImhotepCommand(final String sessionId, final Class<T> resultClass) {
        this.sessionId = sessionId;
        this.resultClass = resultClass;
    }

    protected abstract ImhotepRequestSender imhotepRequestSenderInitializer();

    @Override
    public void writeToOutputStream(final OutputStream os) throws IOException {
        getImhotepRequestSender().writeToStreamNoFlush(os);
    }
}
