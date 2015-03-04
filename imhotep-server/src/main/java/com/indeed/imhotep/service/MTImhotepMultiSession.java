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
 package com.indeed.imhotep.service;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.local.ImhotepLocalSession;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

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
                          final AtomicLong tempFileSizeBytesLeft) throws ImhotepOutOfMemoryException {
        super(sessions, tempFileSizeBytesLeft);

        this.memory = memory;
        this.executor = executor;
        memoryClaimed = 0;

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
    protected <E, T> void execute(final T[] ret, E[] things, final ThrowingFunction<? super E, ? extends T> function) throws ExecutionException {
        final List<Future<T>> futures = Lists.newArrayListWithCapacity(things.length);
        for (final E thing : things) {
            futures.add(executor.submit(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return function.apply(thing);
                }
            }));
        }

        Throwable t = null;

        for (int i = 0; i < futures.size(); ++i) {
            try {
                ret[i] = futures.get(i).get();
            } catch (Throwable t2) {
                t = t2;
            }
        }
        if (t != null) {
            safeClose();
            throw Throwables2.propagate(t, ExecutionException.class);
        }
    }
}
