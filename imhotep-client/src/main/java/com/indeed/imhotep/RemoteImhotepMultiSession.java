/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.indeed.util.core.Pair;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RawFTGSIterator;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jsgroth
 */
public class RemoteImhotepMultiSession extends AbstractImhotepMultiSession {
    private static final Logger log = Logger.getLogger(RemoteImhotepMultiSession.class);

    private final ExecutorService executor;

    private final String sessionId;
    private final InetSocketAddress[] nodes;

    private final long localTempFileSizeLimit;
    private final boolean shutDownExecutorOnClose;

    public RemoteImhotepMultiSession(ImhotepSession[] sessions, final String sessionId, final InetSocketAddress[] nodes,
                                     long localTempFileSizeLimit, AtomicLong tempFileSizeBytesLeft) {
        this(sessions, Executors.newCachedThreadPool(new ThreadFactory() {
            int i = 0;

            @Override
            public Thread newThread(Runnable r) {
                final Thread t = new Thread(r, "MultiSessionThread"+i++);
                t.setDaemon(true);
                return t;
            }
        }), sessionId, nodes, localTempFileSizeLimit, tempFileSizeBytesLeft, true);
    }

    public RemoteImhotepMultiSession(ImhotepSession[] sessions, ExecutorService executor, final String sessionId,
                                     final InetSocketAddress[] nodes, long localTempFileSizeLimit, AtomicLong tempFileSizeBytesLeft, boolean shutDownExecutorOnClose) {
        super(sessions, tempFileSizeBytesLeft);
        
        this.executor = executor;
        this.sessionId = sessionId;
        this.nodes = nodes;
        this.localTempFileSizeLimit = localTempFileSizeLimit;
        this.shutDownExecutorOnClose = shutDownExecutorOnClose;
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields) {
        if (sessions.length == 1) {
            return sessions[0].getFTGSIterator(intFields, stringFields);
        }
        final RawFTGSIterator[] mergers = getFTGSIteratorSplits(intFields, stringFields);
        return new FTGSInterleaver(mergers);
    }

    public RawFTGSIterator[] getFTGSIteratorSplits(final String[] intFields, final String[] stringFields) {
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
                    return session.mergeFTGSSplit(intFields, stringFields, sessionId, nodes, index);
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
        return mergers;
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        if (sessions.length == 1) {
            return sessions[0].getSubsetFTGSIterator(intFields, stringFields);
        }
        final RawFTGSIterator[] mergers = getSubsetFTGSIteratorSplits(intFields, stringFields);
        return new FTGSInterleaver(mergers);
    }

    @Override
    public RawFTGSIterator[] getSubsetFTGSIteratorSplits(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
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
                    return session.mergeSubsetFTGSSplit(intFields, stringFields, sessionId, nodes, index);
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
        return mergers;
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

    /**
     * Returns the number of bytes written to the temp files for this session locally.
     * Returns -1 if tempFileSizeBytesLeft was set to null.
     */
    public long getTempFilesBytesWritten() {
        if(tempFileSizeBytesLeft == null || localTempFileSizeLimit <= 0) {
            return -1;
        }
        return localTempFileSizeLimit - tempFileSizeBytesLeft.get();
    }
}
