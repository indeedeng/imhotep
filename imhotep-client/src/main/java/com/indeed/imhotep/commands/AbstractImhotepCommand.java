package com.indeed.imhotep.commands;

import com.indeed.imhotep.api.CommandSerializationParameters;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Initializes the imhotepRequestSender only when its getter is called, to avoid its creation on the server side.
 */
@EqualsAndHashCode
public abstract class AbstractImhotepCommand<T> implements ImhotepCommand<T> {

    @Getter
    final String sessionId;

    @Getter(lazy = true)
    @EqualsAndHashCode.Exclude
    private final ImhotepRequestSender imhotepRequestSender = imhotepRequestSenderInitializer();

    @Getter
    final Class<T> resultClass;

    AbstractImhotepCommand(final String sessionId, final Class<T> resultClass) {
        this.sessionId = sessionId;
        this.resultClass = resultClass;
    }

    protected abstract ImhotepRequestSender imhotepRequestSenderInitializer();

    @Override
    public void writeToOutputStream(
            final OutputStream os,
            final CommandSerializationParameters serializationParameters
    ) throws IOException {
        getImhotepRequestSender().writeToStreamNoFlush(os);
    }
}
