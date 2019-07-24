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

    public CommandExecutor(final AbstractImhotepMultiSession imhotepMultiSession, final ListeningExecutorService executorService,
                           final ImhotepLocalSession imhotepLocalSession, final List<ImhotepCommand> firstCommands,
                           final ImhotepCommand<T> lastCommand) {
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
                // wraps IOOME with RunTimeException, may have successive wraps later too
                // make sure to propagate to LocalSession with an instanceOf check
                throw Throwables.propagate(e);
            }
        } finally {
            ImhotepTask.clear();
        }
    }

    private ListenableFuture<Object> processCommand(final ImhotepCommand imhotepCommand, final DefUseManager defUseManager) {
        final List<ListenableFuture<Object>> upstreamFutures = defUseManager.getUpstreamFutures(imhotepCommand.getInputGroups(), imhotepCommand.getOutputGroups());
        final ListenableFuture<Object> commandFuture = Futures.transform(Futures.allAsList(upstreamFutures), (final List<Object> ignored) -> applyCommand(imhotepCommand), executorService);

        defUseManager.addUses(imhotepCommand.getInputGroups(), commandFuture);
        defUseManager.addDefinitions(imhotepCommand.getOutputGroups(), commandFuture);

        return commandFuture;
    }

    T processCommands(final DefUseManager defUseManager) throws InterruptedException, ImhotepOutOfMemoryException {
        for (final ImhotepCommand imhotepCommand : firstCommands) {
            processCommand(imhotepCommand, defUseManager);
        }
        final ListenableFuture<Object> lastCommandFuture = processCommand(lastCommand, defUseManager);

        final List<ListenableFuture<Object>> allFutures = defUseManager.getAllDefsUses();
        try {
            Futures.allAsList(allFutures).get();  // wait for all execution to complete
            return (T)lastCommandFuture.get();
        } catch (final ExecutionException e) {
            // IOOME wrapped twice, once by Futures.get() and by applyCommand call in Futures.transform
            Throwables.propagateIfInstanceOf(e.getCause().getCause(), ImhotepOutOfMemoryException.class);
            throw Throwables.propagate(e);
        }
    }
}
