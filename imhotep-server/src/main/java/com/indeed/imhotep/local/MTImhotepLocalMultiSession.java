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
 package com.indeed.imhotep.local;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.hash.MurmurHash;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jsgroth
 */
public class MTImhotepLocalMultiSession extends AbstractImhotepMultiSession<ImhotepLocalSession> {
    private static final Logger log = Logger.getLogger(MTImhotepLocalMultiSession.class);
    private static final int NUM_WORKERS = 8;
    private final AtomicReference<CountDownLatch> writeFTGSSplitReqLatch = new AtomicReference<>();
    private Socket[] ftgsOutputSockets;

    private final ExecutorService ftgsExecutorService = Executors.newFixedThreadPool(NUM_WORKERS, new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("FTGSDelegator-%d")
                    .build()
    );


    private final MemoryReservationContext memory;

    private final ExecutorService executor;

    private final AtomicReference<Boolean> closed = new AtomicReference<>();

    private final long memoryClaimed;

    private final boolean useNativeFtgs;

    private final class FTGSSplitsScheduler implements Callable<Void> {
        private final MultiShardFlamdexReader multiShardFlamdexReader;
        private final String[] intFields;
        private final String[] stringFields;
        private final int numSplits;

        private FTGSSplitsScheduler(MultiShardFlamdexReader multiShardFlamdexReader, String[] intFields, String[] stringFields, int numSplits) {
            this.multiShardFlamdexReader = multiShardFlamdexReader;
            this.intFields = intFields;
            this.stringFields = stringFields;
            this.numSplits = numSplits;
        }

        @Override
        public Void call() throws Exception {
            final CountDownLatch latch = writeFTGSSplitReqLatch.get();
            if (latch == null) {
                throw new IllegalStateException("countdown latch is null!");
            }
            latch.await();
            writeFTGSSplitReqLatch.set(null);
            final List<ConcurrentLinkedQueue<FTGSIterateRequest>> ftgsIterateRequestQueues = Lists.newArrayListWithCapacity(NUM_WORKERS);
            for (int i = 0; i < NUM_WORKERS; i++) {
                ftgsIterateRequestQueues.set(i, new ConcurrentLinkedQueue<FTGSIterateRequest>());
                final FTGSIteratorSplitDelegatingWorker worker = new FTGSIteratorSplitDelegatingWorker(null, ftgsIterateRequestQueues.get(i));//TODO: supply an actual implementation
                ftgsExecutorService.submit(worker);
            }
            for (final String intField : intFields) {
                try (final MultiShardIntTermIterator offsetIterator = multiShardFlamdexReader.intTermOffsetIterator(intField)) {
                    while (offsetIterator.next()) {
                        final long term = offsetIterator.term();
                        final int splitIndex = (int) ((term * FTGSSplitterConstants.LARGE_PRIME_FOR_CLUSTER_SPLIT + 12345 & Integer.MAX_VALUE) >> 16) % numSplits;
                        final int workerId = splitIndex % NUM_WORKERS;
                        final long[] offsets = new long[sessions.length];//there is a local session per shard
                        offsetIterator.offsets(offsets);
                        final FTGSIterateRequest ftgsIterateRequest = FTGSIterateRequest.create(intField, term, offsets, ftgsOutputSockets[splitIndex]);
                        ftgsIterateRequestQueues.get(workerId).offer(ftgsIterateRequest);
                    }
                }
            }
            for (final String stringField : stringFields) {
                try (final MultiShardStringTermIterator offsetIterator = multiShardFlamdexReader.stringTermOffsetIterator(stringField)) {
                    while (offsetIterator.next()) {
                        final String term = offsetIterator.term();
                        final byte[] bytes = term.getBytes(Charsets.UTF_8);
                        final int splitIndex = hashStringTerm(bytes, bytes.length, numSplits);
                        final int workerId = splitIndex % NUM_WORKERS;
                        final long[] offsets = new long[sessions.length];
                        offsetIterator.offsets(offsets);
                        final FTGSIterateRequest ftgsIterateRequest = FTGSIterateRequest.create(stringField, term, offsets, ftgsOutputSockets[splitIndex]);
                        ftgsIterateRequestQueues.get(workerId).offer(ftgsIterateRequest);
                    }
                }
            }
            for (int i = 0; i < NUM_WORKERS; i++) {
                ftgsIterateRequestQueues.get(i).offer(FTGSIterateRequest.END);
            }
            return null;
        }
    }

    private int hashStringTerm(final byte[] termStringBytes, final int termStringLength, final int numSplits) {
        return ((MurmurHash.hash32(termStringBytes, 0, termStringLength)*FTGSSplitterConstants.LARGE_PRIME_FOR_CLUSTER_SPLIT+12345 & 0x7FFFFFFF) >> 16) % numSplits;
    }

    public MTImhotepLocalMultiSession(final ImhotepLocalSession[] sessions,
                                      final MemoryReservationContext memory,
                                      final ExecutorService executor,
                                      final AtomicLong tempFileSizeBytesLeft, boolean useNativeFtgs) throws ImhotepOutOfMemoryException {
        super(sessions, tempFileSizeBytesLeft);
        this.useNativeFtgs = useNativeFtgs;
        this.memory = memory;
        this.executor = executor;
        memoryClaimed = 0;

        if (!memory.claimMemory(memoryClaimed)) {
            //noinspection NewExceptionWithoutArguments
            throw new ImhotepOutOfMemoryException();
        }
    }
    
    @Override
    protected void preClose() {
        if (closed.compareAndSet(false, true)) {
            try {
                super.preClose();
                closeFTGSSockets();
            } finally {
                memory.releaseMemory(memoryClaimed);
                // don't want to shut down the executor since it is re-used
            }
        }
    }

    private void closeFTGSSockets() {
        for (final Socket socket : ftgsOutputSockets) {
            if (socket != null && !socket.isClosed()) {
                Closeables2.closeQuietly(socket, log);
            }
        }
    }

    @Override
    public void writeFTGSIteratorSplit(String[] intFields, String[] stringFields, int splitIndex, final int numSplits, final Socket socket) {
        CountDownLatch latch = writeFTGSSplitReqLatch.get();
        if (latch == null) {
            if (writeFTGSSplitReqLatch.compareAndSet(null, new CountDownLatch(numSplits))) {
                ftgsOutputSockets = new Socket[numSplits];
                ftgsExecutorService.submit(new FTGSSplitsScheduler(new MultiShardFlamdexReader(getFlamdexReaders()), intFields, stringFields, numSplits));
                latch = writeFTGSSplitReqLatch.get();
            } else {
                latch = writeFTGSSplitReqLatch.get();
            }
        }
        ftgsOutputSockets[splitIndex] = socket;
        //check if the session was closed, and if it was, try to close this socket and throw an exception
        if (closed.get()) {
            closeFTGSSockets();
            throw new IllegalStateException("the session was closed before getting all the splits!");
        }
        if (latch.getCount() == 0) {
            throw new IllegalStateException("Latch was already set to zero!");
        }
        latch.countDown();
    }

    private SimpleFlamdexReader[] getFlamdexReaders() {
        final SimpleFlamdexReader[] ret = new SimpleFlamdexReader[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            final FlamdexReader flamdexReader = sessions[i].getReader();
            if (flamdexReader instanceof SimpleFlamdexReader) {
                ret[i] = (SimpleFlamdexReader) flamdexReader;
            } else {
                throw new RuntimeException("FlamdexReader is not of type "+SimpleFlamdexReader.class.getCanonicalName());
            }
        }
        return ret;
    }

    @Override
    protected void postClose() {
        if (memory.usedMemory() > 0) {
            log.error("MTImhotepMultiSession is leaking! usedMemory = "+memory.usedMemory());
        }
        Closeables2.closeQuietly(memory, log);
    }

    @Override
    protected ImhotepRemoteSession createImhotepRemoteSession(InetSocketAddress address, String sessionId, AtomicLong tempFileSizeBytesLeft) {
        return new ImhotepRemoteSession(address.getHostName(), address.getPort(), sessionId, tempFileSizeBytesLeft, useNativeFtgs);
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
            } catch (final Throwable t2) {
                t = t2;
            }
            if (t != null) {
                safeClose();
                throw Throwables2.propagate(t, ExecutionException.class);
            }
        }
    }
}
