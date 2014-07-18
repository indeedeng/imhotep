package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.indeed.util.core.Pair;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RawFTGSIterator;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author jsgroth
 */
public class RemoteImhotepMultiSession extends AbstractImhotepMultiSession {
    private static final Logger log = Logger.getLogger(RemoteImhotepMultiSession.class);

    private final ExecutorService executor;

    private final String sessionId;
    private final InetSocketAddress[] nodes;

    private final boolean shutDownExecutorOnClose;

    public RemoteImhotepMultiSession(ImhotepSession[] sessions, int numSplits, final String sessionId, final InetSocketAddress[] nodes) {
        this(sessions, Executors.newCachedThreadPool(new ThreadFactory() {
            int i = 0;

            @Override
            public Thread newThread(Runnable r) {
                final Thread t = new Thread(r, "MultiSessionThread"+i++);
                t.setDaemon(true);
                return t;
            }
        }), numSplits, sessionId, nodes, true);
    }

    public RemoteImhotepMultiSession(ImhotepSession[] sessions, ExecutorService executor, int numSplits, final String sessionId, final InetSocketAddress[] nodes, boolean shutDownExecutorOnClose) {
        super(sessions, numSplits);
        
        this.executor = executor;
        this.sessionId = sessionId;
        this.nodes = nodes;
        this.shutDownExecutorOnClose = shutDownExecutorOnClose;
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields) {
        if (sessions.length == 1) {
            return sessions[0].getFTGSIterator(intFields, stringFields);
        }
        final Pair<Integer, ImhotepSession>[] indexesAndSessions = new Pair[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            indexesAndSessions[i] = Pair.of(i, sessions[i]);
        }
        final RawFTGSIterator[] mergers = new RawFTGSIterator[sessions.length];
        try {
            execute(mergers, indexesAndSessions, new ThrowingFunction<Pair<Integer, ImhotepSession>, RawFTGSIterator>() {
                public RawFTGSIterator apply(final Pair<Integer, ImhotepSession> indexSessionPair) throws Exception {
                    final ImhotepSession session = indexSessionPair.getSecond();
                    final int index = indexSessionPair.getFirst();
                    return new BufferedFTGSIterator(executor, session.mergeFTGSSplit(intFields, stringFields, sessionId, nodes, index), numStats, true);
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
        return new RawFTGSMerger(Arrays.asList(mergers), numStats, null);
    }

    @Override
    protected void postClose() {
        if (shutDownExecutorOnClose) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                    log.warn("executor did not shut down, continuing anyway");
                }
            } catch (InterruptedException e) {
                log.warn(e);
            }
        }
    }

    @Override
    protected <E, T> void execute(final T[] ret, E[] things, final ThrowingFunction<? super E, ? extends T> function) throws ExecutionException {
        final List<Future<T>> futures = new ArrayList<Future<T>>(things.length);
        for (final E thing : things) {
            futures.add(executor.submit(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return function.apply(thing);
                }
            }));
        }

        try {
            for (int i = 0; i < futures.size(); ++i) {
                final Future<T> future = futures.get(i);
                ret[i] = future.get();
            }
        } catch (ExecutionException e) {
            safeClose();
            throw e;
        } catch (InterruptedException e) {
            safeClose();
            throw new RuntimeException(e);
        }
    }
}
