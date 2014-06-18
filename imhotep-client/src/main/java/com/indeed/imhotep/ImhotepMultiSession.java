package com.indeed.imhotep;

import com.indeed.imhotep.api.ImhotepSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
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
public class ImhotepMultiSession extends AbstractImhotepMultiSession {
    private static final Logger log = Logger.getLogger(ImhotepMultiSession.class);

    private final ExecutorService executor;

    private final boolean shutDownExecutorOnClose;

    public ImhotepMultiSession(ImhotepSession[] sessions, int numSplits) {
        this(sessions, Executors.newCachedThreadPool(new ThreadFactory() {
            int i = 0;

            @Override
            public Thread newThread(Runnable r) {
                final Thread t = new Thread(r, "MultiSessionThread"+i++);
                t.setDaemon(true);
                return t;
            }
        }), numSplits, true);
    }

    public ImhotepMultiSession(ImhotepSession[] sessions, ExecutorService executor, int numSplits, boolean shutDownExecutorOnClose) {
        super(sessions, numSplits);
        
        this.executor = executor;
        this.shutDownExecutorOnClose = shutDownExecutorOnClose;
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
    protected <T> void execute(final T[] ret, final ThrowingFunction<? super ImhotepSession, ? extends T> function) throws ExecutionException {
        final List<Future<T>> futures = new ArrayList<Future<T>>(sessions.length);
        for (final ImhotepSession session : sessions) {
            futures.add(executor.submit(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return function.apply(session);
                }
            }));
        }

        try {
            for (int i = 0; i < sessions.length; ++i) {
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
