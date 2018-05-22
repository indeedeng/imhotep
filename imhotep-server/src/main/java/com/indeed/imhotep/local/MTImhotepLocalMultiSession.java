/*
 * Copyright (C) 2018 Indeed Inc.
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

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.FTGSSplitter;
import com.indeed.imhotep.GroupStatsDummyIterator;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.RawFTGSMerger;
import com.indeed.imhotep.TermLimitedRawFTGSIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.io.SocketUtils;
import com.indeed.imhotep.pool.GroupStatsPool;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


/**
 * @author jsgroth
 */
public class MTImhotepLocalMultiSession extends AbstractImhotepMultiSession<ImhotepLocalSession> {
    private static final Logger log = Logger.getLogger(MTImhotepLocalMultiSession.class);

    static {
        loadNativeLibrary();
        log.info("libftgs loaded");
        log.info("Using SSSE3! (if the processor in this computer doesn't support SSSE3 "
                         + "this process will fail with SIGILL)");
    }

    public static void loadNativeLibrary() {
        try {
            final String osName = System.getProperty("os.name");
            final String arch = System.getProperty("os.arch");
            final String resourcePath = "/native/" + osName + "-" + arch + "/libftgs.so.1.0.1";
            final InputStream is = MTImhotepLocalMultiSession.class.getResourceAsStream(resourcePath);
            if (is == null) {
                throw new FileNotFoundException(
                        "unable to find libftgs.so.1.0.1 at resource path " + resourcePath);
            }
            final File tempFile = File.createTempFile("libftgs", ".so");
            final OutputStream os = new FileOutputStream(tempFile);
            ByteStreams.copy(is, os);
            os.close();
            is.close();
            System.load(tempFile.getAbsolutePath());
            tempFile.delete();
        } catch (final Throwable e) {
            e.printStackTrace();
            log.warn("unable to load libftgs using class loader, looking in java.library.path", e);
            System.loadLibrary("ftgs"); // if this fails it throws UnsatisfiedLinkError
        }
    }

    private final AtomicReference<CyclicBarrier> writeFTGSSplitBarrier = new AtomicReference<>();
    private final Socket[] ftgsOutputSockets = new Socket[256];

    private final MemoryReservationContext memory;

    private final AtomicReference<Boolean> closed = new AtomicReference<>();

    private final long memoryClaimed;

    private final boolean useNativeFtgs;

    private boolean onlyBinaryMetrics;

    protected FTGSSplitter[] ftgsIteratorSplitters;

    public MTImhotepLocalMultiSession(final ImhotepLocalSession[] sessions,
                                      final MemoryReservationContext memory,
                                      final AtomicLong tempFileSizeBytesLeft,
                                      final boolean useNativeFtgs,
                                      final String userName,
                                      final String clientName)
        throws ImhotepOutOfMemoryException {
        super(sessions, tempFileSizeBytesLeft, userName, clientName);
        this.useNativeFtgs = useNativeFtgs;
        this.memory = memory;
        this.memoryClaimed = 0;
        this.closed.set(false);

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
            } finally {
                closeFTGSSockets();
                memory.releaseMemory(memoryClaimed);
                // don't want to shut down the executor since it is re-used
            }
        }
    }

    /**
     * Closes the sockets silently. Guaranteed to not throw Exceptions
     */
    private void closeFTGSSockets() {
        Closeables2.closeAll(Arrays.asList(ftgsOutputSockets), log);
    }

    /* !@# Blech! This is kludgy as all get out, but it's what we have for now.

       PreConditions:
       - All contained sessions are ImhotepNativeLocalSessions

       PostConditions:
       - Sessions will have valid multicaches (aka packed tables) within.
       - Sessions will have valid NativeShards within.
    */
    public NativeShard[] updateMulticaches() throws ImhotepOutOfMemoryException {
        final NativeShard[]    result        = new NativeShard[sessions.length];
        final MultiCacheConfig config        = new MultiCacheConfig();
        final StatLookup[]     sessionsStats = new StatLookup[sessions.length];

        for (int i = 0; i < sessions.length; i++) {
            sessionsStats[i] = sessions[i].statLookup;
        }
        config.calcOrdering(sessionsStats, sessions[0].numStats);
        this.onlyBinaryMetrics = config.isOnlyBinaryMetrics();

        executeMemoryException(result, new ThrowingFunction<ImhotepSession, NativeShard>() {
            @Override
            public NativeShard apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                final ImhotepNativeLocalSession inls = (ImhotepNativeLocalSession) session;
                inls.buildMultiCache(config);
                inls.bindNativeReferences(); // !@# blech!!!
                return inls.getNativeShard();
            }
        });
        return result;
    }

    @Override
    public long[] getGroupStats(final int stat) {

        final GroupStatsPool pool = new GroupStatsPool(numGroups);

        executeRuntimeException(nullBuf, session -> {
            final long[] buffer = pool.getBuffer();
            session.addGroupStats(stat, buffer);
            pool.returnBuffer(buffer);
            return null;
        });

        // TODO: run as a separate task maybe?
        return pool.getTotalResult();
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final int stat) {
        // there is two ways to create GroupStatsIterator in multisession:
        // create iterator over result of getGroupStats method or create merger for iterators.
        // In case of local multisession we are keeping full result in memory anyway,
        // so first option is better.
        return new GroupStatsDummyIterator(getGroupStats(stat));
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, final int sortStat) {
        final boolean topTermsByStat = sortStat >= 0;
        final long perSplitTermLimit = topTermsByStat ? 0 : termLimit;
        return mergeFTGSIteratorsForSessions(sessions, termLimit, sortStat, s -> s.getFTGSIterator(intFields, stringFields, perSplitTermLimit));
    }

    @Override
    public RawFTGSIterator[] getFTGSIteratorSplits(String[] intFields, String[] stringFields, long termLimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        return mergeFTGSIteratorsForSessions(sessions, -1, -1, s -> s.getSubsetFTGSIterator(intFields, stringFields));
    }

    @Override
    public RawFTGSIterator[] getSubsetFTGSIteratorSplits(
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized RawFTGSIterator getFTGSIteratorSplit(final String[] intFields, final String[] stringFields, final int splitIndex, final int numSplits, final long termLimit) {
        if (ftgsIteratorSplitters == null || (ftgsIteratorSplitters.length == 0 && sessions.length != 0) ||
                ftgsIteratorSplitters[0].isClosed()) {

            ftgsIteratorSplitters = new FTGSSplitter[sessions.length];
            try {
                executeSessions(getSplitBufferThreads, ftgsIteratorSplitters, false, imhotepSession -> imhotepSession.getFTGSIteratorSplitter(intFields, stringFields, numSplits, termLimit));
            } catch (final Throwable t) {
                Closeables2.closeAll(log, ftgsIteratorSplitters);
                throw Throwables.propagate(t);
            }
        }

        final List<RawFTGSIterator> splits = Arrays.stream(ftgsIteratorSplitters)
                .map(splitter -> splitter.getFtgsIterators()[splitIndex]).collect(Collectors.toList());

        if (sessions.length == 1) {
            return splits.get(0);
        }
        RawFTGSIterator merger = new RawFTGSMerger(splits, numStats, null);
        if(termLimit > 0) {
            merger = new TermLimitedRawFTGSIterator(merger, termLimit);
        }
        return merger;
    }

    @Override
    public synchronized RawFTGSIterator getSubsetFTGSIteratorSplit(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final int splitIndex, final int numSplits) {
        if (ftgsIteratorSplitters == null || (ftgsIteratorSplitters.length == 0 && sessions.length != 0) ||
                ftgsIteratorSplitters[0].isClosed()) {

            ftgsIteratorSplitters = new FTGSSplitter[sessions.length];
            try {
                executeSessions(getSplitBufferThreads, ftgsIteratorSplitters, false, imhotepSession -> imhotepSession.getSubsetFTGSIteratorSplitter(intFields, stringFields, numSplits));
            } catch (final Throwable t) {
                Closeables2.closeAll(log, ftgsIteratorSplitters);
                throw Throwables.propagate(t);
            }
        }
        final List<RawFTGSIterator> splits = Arrays.stream(ftgsIteratorSplitters)
                .map(splitter -> splitter.getFtgsIterators()[splitIndex]).collect(Collectors.toList());

        if (sessions.length == 1) {
            return splits.get(0);
        }
        return new RawFTGSMerger(splits, numStats, null);
    }

    @Override
    public void writeFTGSIteratorSplit(final String[] intFields,
                                       final String[] stringFields,
                                       final int splitIndex,
                                       final int numSplits,
                                       final long termLimit,
                                       final Socket socket) throws ImhotepOutOfMemoryException {

        // TODO: implement the FTGS term limit in native code
        if (termLimit > 0) {
            throw new IllegalArgumentException("FTGS termLimit is not supported in native mode yet");
        }

        // save socket
        ftgsOutputSockets[splitIndex] = socket;

        final CyclicBarrier newBarrier = new CyclicBarrier(numSplits, new Runnable() {
                @Override
                public void run() {
                    final NativeShard[] shards;
                    try {
                        shards = updateMulticaches();
                    }
                    catch (final ImhotepOutOfMemoryException e) {
                        throw new RuntimeException(e);
                    }

                    final NativeFTGSRunnable nativeRunnable =
                        new NativeFTGSRunnable(shards, onlyBinaryMetrics,
                                               intFields, stringFields, getNumGroups(),
                                               numStats, numSplits, ftgsOutputSockets);
                    nativeRunnable.run();
                }
        });

        // agree on a single barrier
        CyclicBarrier barrier = writeFTGSSplitBarrier.get();
        if (barrier == null) {
            if (writeFTGSSplitBarrier.compareAndSet(null, newBarrier)) {
                barrier = writeFTGSSplitBarrier.get();
            } else {
                barrier = writeFTGSSplitBarrier.get();
            }
        }

        //There is a potential race condition between ftgsOutputSockets[i] being
        // assigned and the sockets being closed when <code>close()</code> is
        // called. If this method is called concurrently with close it's
        // possible that ftgsOutputSockets[i] will be assigned after it has
        // already been determined to be null in close. This will cause the
        // socket to not be closed. By checking if the session is closed after
        // the assignment we guarantee that either close() will close all
        // sockets correctly (if closed is false here) or that we will close all
        // the sockets if the session was closed simultaneously with this method
        // being called (if closed is true here)
        if (closed.get()) {
            closeFTGSSockets();
            throw new IllegalStateException("the session was closed before getting all the splits!");
        }

        // now run the ftgs on the final thread
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }

        // now reset the barrier value (yes, every thread will do it)
        writeFTGSSplitBarrier.set(null);
    }

    @Override
    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        if (sessions.length == 1) {
            return sessions[0].getDistinct(field, isIntField);
        }
        // It's a hack.
        // We don't care about stats while calculating distinct.
        // And FTGS with no-stats is faster.
        // So we drop stats count, calculate distinct and return stats.
        final int[] savedNumStats = new int[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            savedNumStats[i] = sessions[i].numStats;
        }
        final GroupStatsIterator result;
        try {
            // drop stats.
            for (final ImhotepLocalSession session : sessions) {
                session.numStats = 0;
            }
            final String[] intFields = isIntField ? new String[]{field} : new String[0];
            final String[] strFields = isIntField ? new String[0] : new String[]{field};
            final FTGSIterator iterator = getFTGSIterator(intFields, strFields);
            result = FTGSIteratorUtil.calculateDistinct(iterator, getNumGroups());
        } finally {
            // return stats back.
            for (int i = 0; i < sessions.length; i++) {
                sessions[i].numStats = savedNumStats[i];
            }
        }
        return result;
    }

    @Override
    protected void postClose() {
        if (memory.usedMemory() > 0) {
            log.error("MTImhotepMultiSession is leaking! usedMemory = "+memory.usedMemory());
        }
        Closeables2.closeQuietly(memory, log);
        super.postClose();
    }

    @Override
    protected ImhotepRemoteSession createImhotepRemoteSession(final InetSocketAddress address,
                                                              final String sessionId,
                                                              final AtomicLong tempFileSizeBytesLeft) {
        return new ImhotepRemoteSession(address.getHostName(), address.getPort(),
                                        sessionId, tempFileSizeBytesLeft, useNativeFtgs);
    }

    private static class NativeFTGSRunnable {

        final long[]   shardPtrs;
        final boolean  onlyBinaryMetrics;
        final String[] intFields;
        final String[] stringFields;
        final String   splitsDir;
        final int      numGroups;
        final int      numStats;
        final int      numSplits;
        final int      numWorkers;
        final int[]    socketFDs;

        NativeFTGSRunnable(final NativeShard[] nativeShards,
                           final boolean       onlyBinaryMetrics,
                           final String[]      intFields,
                           final String[]      stringFields,
                           final int           numGroups,
                           final int           numStats,
                           final int           numSplits,
                           final Socket[]      sockets) {

            this.shardPtrs = new long[nativeShards.length];
            for (int idx = 0; idx < nativeShards.length; ++idx) {
                this.shardPtrs[idx] = nativeShards[idx].getPtr();
            }

            this.onlyBinaryMetrics = onlyBinaryMetrics;
            this.intFields         = intFields;
            this.stringFields      = stringFields;
            this.splitsDir         = System.getProperty("java.io.tmpdir", "/dev/null");
            this.numGroups         = numGroups;
            this.numStats          = numStats;
            this.numSplits         = numSplits;

            final String numWorkersStr = System.getProperty("imhotep.ftgs.num.workers", "8");
            this.numWorkers = Integer.parseInt(numWorkersStr);

            final ArrayList<Integer> socketFDArray = new ArrayList<>();
            for (final Socket socket: sockets) {
                final Integer fd = SocketUtils.getOutputDescriptor(socket);
                if (fd >= 0) {
                    socketFDArray.add(fd);
                }
            }
            this.socketFDs = new int[socketFDArray.size()];
            for (int index = 0; index < this.socketFDs.length; ++index) {
                this.socketFDs[index] = socketFDArray.get(index);
            }
        }

        native void run();
    }
}
