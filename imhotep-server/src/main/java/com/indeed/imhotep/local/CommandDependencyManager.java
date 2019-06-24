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
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class CommandDependencyManager<T> {

    static final Logger log = Logger.getLogger(CommandDependencyManager.class);

    private final AbstractImhotepMultiSession imhotepMultiSession;
    private final ListeningExecutorService executorService;
    private final ImhotepLocalSession imhotepLocalSession;

    public CommandDependencyManager(final AbstractImhotepMultiSession imhotepMultiSession, final ListeningExecutorService executorService, final ImhotepLocalSession imhotepLocalSession) {
        this.imhotepMultiSession = imhotepMultiSession;
        this.executorService = executorService;
        this.imhotepLocalSession = imhotepLocalSession;
    }

    private <T> T getCommandApplyValue(final ImhotepCommand<T> imhotepCommand, final List<Object> commandReturnValues) {
        try (final SilentCloseable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
            ImhotepTask.setup(imhotepMultiSession);
            try {
                return imhotepCommand.apply(imhotepLocalSession);
            } catch (final ImhotepOutOfMemoryException e) {
                Throwables.propagate(e);
            }
        }
        return null;
    }

    private <T> ListenableFuture<T> processLastCommand(final ImhotepCommand<T> imhotepCommand, final DefUseManager defUseManager) {
        final List<ListenableFuture<Object>> dependentFutures = defUseManager.getAllUses();
        dependentFutures.addAll(defUseManager.getAllDef());
        final ListenableFuture<T> commandFuture = Futures.transform(Futures.allAsList(dependentFutures), (final List<Object> commandReturnValues) -> getCommandApplyValue(imhotepCommand, commandReturnValues), executorService);
        return commandFuture;
    }

    private ListenableFuture<Object> processCommand(final ImhotepCommand imhotepCommand, final DefUseManager defUseManager) {
        final List<ListenableFuture<Object>> dependentFutures = defUseManager.getDependentFutures(imhotepCommand.getInputGroups(), imhotepCommand.getOutputGroup());
        final ListenableFuture<Object> commandFuture = Futures.transform(Futures.allAsList(dependentFutures), (final List<Object> commandReturnValues) -> getCommandApplyValue(imhotepCommand, commandReturnValues), executorService);

        defUseManager.addUses(imhotepCommand.getInputGroups(), commandFuture);
        defUseManager.addDefinition(imhotepCommand.getOutputGroup(), commandFuture);

        return commandFuture;
    }

    T processCommands(final List<ImhotepCommand> firstCommands, final ImhotepCommand<T> lastCommand, final DefUseManager defUseManager) throws ExecutionException, InterruptedException {
        for (final ImhotepCommand imhotepCommand : firstCommands) {
            processCommand(imhotepCommand, defUseManager);
        }
        return processLastCommand(lastCommand, defUseManager).get();
    }
}
