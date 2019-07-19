package com.indeed.imhotep.local;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.scheduling.ImhotepTask;
import com.indeed.imhotep.scheduling.SilentCloseable;
import com.indeed.imhotep.scheduling.TaskScheduler;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Executes the Commands. Execution Order is decided based on def-use graph of named input groups and output groups.
 * A command is executed only after
 * <ul>
 *     <li> Execution of definitions of input groups and </li>
 *     <li> Execution of definition and uses of output groups </li>
 * </ul>
 * completes execution.
 * This list of upstream ListenableFutures is returned by {@link DefUseManager#getUpstreamFutures(List, List)}
 */
public class CommandExecutor<T> {

    private final AbstractImhotepMultiSession imhotepMultiSession;
    private final ListeningExecutorService executorService;
    private final ImhotepLocalSession imhotepLocalSession;
    private final List<ImhotepCommand> firstCommands;
    private final ImhotepCommand<T> lastCommand;

    public CommandExecutor(final AbstractImhotepMultiSession imhotepMultiSession, final ListeningExecutorService executorService, final ImhotepLocalSession imhotepLocalSession, final List<ImhotepCommand> firstCommands, final ImhotepCommand<T> lastCommand) {
        this.imhotepMultiSession = imhotepMultiSession;
        this.executorService = executorService;
        this.imhotepLocalSession = imhotepLocalSession;
        this.firstCommands = firstCommands;
        this.lastCommand = lastCommand;
    }

    private Object applyCommand(final ImhotepCommand imhotepCommand) {
        ImhotepTask.setup(imhotepMultiSession, imhotepCommand);
        ImhotepTask.registerInnerSession(imhotepLocalSession);
        try (final SilentCloseable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
            try {
                return imhotepCommand.apply(imhotepLocalSession);
            } catch (final ImhotepOutOfMemoryException e) {
                Throwables.propagate(e);
            }
        }
        ImhotepTask.clear();
        return null;
    }

    private ListenableFuture<Object> processCommand(final ImhotepCommand imhotepCommand, final DefUseManager defUseManager) {
        final List<ListenableFuture<Object>> upstreamFutures = defUseManager.getUpstreamFutures(imhotepCommand.getInputGroups(), imhotepCommand.getOutputGroups());
        final ListenableFuture<Object> commandFuture = Futures.transform(Futures.allAsList(upstreamFutures), (final List<Object> ignored) -> applyCommand(imhotepCommand), executorService);

        defUseManager.addUses(imhotepCommand.getInputGroups(), commandFuture);
        defUseManager.addDefinitions(imhotepCommand.getOutputGroups(), commandFuture);

        return commandFuture;
    }

    T processCommands(final DefUseManager defUseManager) throws ExecutionException, InterruptedException, ImhotepOutOfMemoryException {
        for (final ImhotepCommand imhotepCommand : firstCommands) {
            processCommand(imhotepCommand, defUseManager);
        }
        final ListenableFuture<Object> lastCommandFuture = processCommand(lastCommand, defUseManager);

        final List<ListenableFuture<Object>> allFutures = defUseManager.getAllDefsUses();
        try {
            return  (T) Futures.transform(Futures.allAsList(allFutures), (final List<Object> inputs) -> inputs.get(inputs.indexOf(lastCommandFuture)) , executorService).get();
        } catch (final ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), ImhotepOutOfMemoryException.class);
            throw e;
        }
    }
}
