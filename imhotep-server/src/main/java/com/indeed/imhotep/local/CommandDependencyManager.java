package com.indeed.imhotep.local;

import com.google.common.base.Preconditions;
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
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class CommandDependencyManager<T> {

    static final Logger log = Logger.getLogger(CommandDependencyManager.class);

    private final AbstractImhotepMultiSession imhotepMultiSession;
    private final ListeningExecutorService executorService;
    private final ImhotepLocalSession imhotepLocalSession;
    private final Map<ImhotepCommand, Object> commandOutputs;

    public CommandDependencyManager(final AbstractImhotepMultiSession imhotepMultiSession, final ListeningExecutorService executorService, final ImhotepLocalSession imhotepLocalSession) {
        this.imhotepMultiSession = imhotepMultiSession;
        this.executorService = executorService;
        this.imhotepLocalSession = imhotepLocalSession;
        this.commandOutputs = new HashMap<>();
    }

    private Void commandApply(final ImhotepCommand<T> imhotepCommand) {
        try (final SilentCloseable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
            ImhotepTask.setup(imhotepMultiSession);
            try {
                Preconditions.checkArgument(!commandOutputs.containsKey(imhotepCommand), "Tried executing imhotepCommand " + imhotepCommand + " twice.");
                commandOutputs.put(imhotepCommand, imhotepCommand.apply(imhotepLocalSession));
            } catch (final ImhotepOutOfMemoryException e) {
                Throwables.propagate(e);
            }
        }
        return null;
    }

    private ListenableFuture<Void> processCommand(final ImhotepCommand imhotepCommand, final DefUseManager defUseManager) {
        final List<ListenableFuture<Void>> dependentFutures = defUseManager.getDependentFutures(imhotepCommand.getInputGroups(), imhotepCommand.getOutputGroups());
        final ListenableFuture<Void> commandFuture = Futures.transform(Futures.allAsList(dependentFutures), (final List<Void> ignored) -> commandApply(imhotepCommand), executorService);

        defUseManager.addUses(imhotepCommand.getInputGroups(), commandFuture);
        defUseManager.addDefinition(imhotepCommand.getOutputGroups(), commandFuture);

        return commandFuture;
    }

    T processCommands(final List<ImhotepCommand> firstCommands, final ImhotepCommand<T> lastCommand, final DefUseManager defUseManager) throws ExecutionException, InterruptedException {
        for (final ImhotepCommand imhotepCommand : firstCommands) {
            processCommand(imhotepCommand, defUseManager);
        }
        processCommand(lastCommand, defUseManager);

        final List<ListenableFuture<Void>> allFutures = defUseManager.getAllFutures();
        for (final ListenableFuture<Void> future: allFutures) {
            future.get();
        }

        return (T)commandOutputs.get(lastCommand);
    }
}
