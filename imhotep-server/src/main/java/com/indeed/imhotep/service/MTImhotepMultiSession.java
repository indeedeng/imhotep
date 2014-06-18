package com.indeed.imhotep.service;

import com.google.common.collect.Lists;
import com.indeed.util.core.io.Closeables2;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.local.ImhotepLocalSession;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author jsgroth
 */
class MTImhotepMultiSession extends AbstractImhotepMultiSession {

    private static final Logger log = Logger.getLogger(MTImhotepMultiSession.class);

    private final MemoryReservationContext memory;

    private final ExecutorService executor;

    private final Object closeLock = new Object();
    private boolean closed = false;

    private final long memoryClaimed;
    

    MTImhotepMultiSession(final ImhotepLocalSession[] sessions,
                          final MemoryReservationContext memory,
                          final ExecutorService executor,
                          int numSplits) throws ImhotepOutOfMemoryException {
        super(sessions, numSplits);

        this.memory = memory;
        this.executor = executor;
        memoryClaimed = sessions.length*numSplits*SPLIT_BUFFER_SIZE+numSplits*MERGE_BUFFER_SIZE;

        if (!memory.claimMemory(memoryClaimed)) throw new ImhotepOutOfMemoryException();
    }
    
    @Override
    protected void preClose() {

        synchronized (closeLock) {
            if (!closed) {
                closed = true;
                try {
                    super.preClose();
                } finally {
                    memory.releaseMemory(memoryClaimed);
                    // don't want to shut down the executor since it is re-used
                }
            }
        }
    }

    @Override
    protected void postClose() {
        if (memory.usedMemory() > 0) {
            log.error("MTImhotepMultiSession is leaking! usedMemory = "+memory.usedMemory());
        }
        Closeables2.closeQuietly(memory, log);
    }

    @Override
    protected <T> void execute(final T[] ret, final ThrowingFunction<? super ImhotepSession, ? extends T> function) throws ExecutionException {
        final List<Future<T>> futures = Lists.newArrayListWithCapacity(sessions.length);
        for (final ImhotepSession session : sessions) {
            futures.add(executor.submit(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return (T) function.apply(session);
                }
            }));
        }

        for (int i = 0; i < sessions.length; ++i) {
            try {
                ret[i] = futures.get(i).get();
            } catch (ExecutionException e) {
                safeClose();
                throw e;
            } catch (InterruptedException e) {
                safeClose();
                throw new RuntimeException(e);
            }
        }
    }
}
