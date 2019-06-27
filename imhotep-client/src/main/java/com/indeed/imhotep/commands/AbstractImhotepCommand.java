package com.indeed.imhotep.commands;

import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    final List<String> inputGroups;
    final List<String> outputGroups;

    AbstractImhotepCommand(final String sessionId, final Class<T> resultClass, final RegroupParams regroupParams) {
        this.sessionId = sessionId;
        this.resultClass = resultClass;
        this.inputGroups = Collections.singletonList(regroupParams.getInputGroups());
        this.outputGroups = Collections.singletonList(regroupParams.getOutputGroups());
    }

    AbstractImhotepCommand(final String sessionId, final Class<T> resultClass, final List<String> inputGroups, final List<String> outputGroups) {
        this.sessionId = sessionId;
        this.resultClass = resultClass;
        this.inputGroups = inputGroups;
        this.outputGroups = outputGroups;
    }

    protected abstract ImhotepRequestSender imhotepRequestSenderInitializer();

    @Override
    public void writeToOutputStream(final OutputStream os) throws IOException {
        getImhotepRequestSender().writeToStreamNoFlush(os);
    }

    @Override
    public List<String> getInputGroups() {
        return inputGroups;
    }

    @Override
    public List<String> getOutputGroups() {
        return outputGroups;
    }
}
