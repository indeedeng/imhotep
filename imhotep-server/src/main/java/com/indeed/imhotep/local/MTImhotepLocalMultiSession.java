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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.HasMapCache;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.SocketUtils;
import com.indeed.imhotep.io.caching.CachedFile;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
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
        } catch (Throwable e) {
            e.printStackTrace();
            log.warn("unable to load libftgs using class loader, looking in java.library.path", e);
            System.loadLibrary("ftgs"); // if this fails it throws UnsatisfiedLinkError
        }
    }

    private final AtomicReference<CyclicBarrier> writeFTGSSplitBarrier = new AtomicReference<>();
    private Socket[] ftgsOutputSockets = new Socket[256];

    private final MemoryReservationContext memory;

    private final ExecutorService ftgsExecutor;

    private final AtomicReference<Boolean> closed = new AtomicReference<>();

    private final long memoryClaimed;

    private final boolean useNativeFtgs;

    private boolean onlyBinaryMetrics;

    public MTImhotepLocalMultiSession(final ImhotepLocalSession[] sessions,
                                      final MemoryReservationContext memory,
                                      final AtomicLong tempFileSizeBytesLeft,
                                      boolean useNativeFtgs)
        throws ImhotepOutOfMemoryException {
        super(sessions, tempFileSizeBytesLeft);
        this.useNativeFtgs = useNativeFtgs;
        this.memory = memory;
        this.memoryClaimed = 0;
        this.closed.set(false);

        final ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setDaemon(true);
        builder.setNameFormat("Native-FTGS-Thread -%d");
        this.ftgsExecutor = Executors.newCachedThreadPool(builder.build());

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

    public MultiCache[] updateMulticaches() throws ImhotepOutOfMemoryException {
        final MultiCache[] multiCaches = new MultiCache[sessions.length];
        final MultiCacheConfig config = new MultiCacheConfig();
        final StatLookup[] sessionsStats = new StatLookup[sessions.length];

        for (int i = 0; i < sessions.length; i++) {
            sessionsStats[i] = sessions[i].statLookup;
        }
        config.calcOrdering(sessionsStats, sessions[0].numStats);
        this.onlyBinaryMetrics = config.isOnlyBinaryMetrics();

        executeMemoryException(multiCaches, new ThrowingFunction<ImhotepSession, MultiCache>() {
            @Override
            public MultiCache apply(ImhotepSession session) throws Exception {
                return ((ImhotepNativeLocalSession)session).buildMultiCache(config);
            }
        });

        return multiCaches;
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
                    MultiCache[] nativeCaches;
                    try {
                        nativeCaches = updateMulticaches();
                    } catch (ImhotepOutOfMemoryException e) {
                        throw new RuntimeException(e);
                    }
                    final FlamdexReader[] readers = new FlamdexReader[sessions.length];
                    for (int i = 0; i < sessions.length; i++) {
                        readers[i] = sessions[i].getReader();
                    }
                    final NativeFTGSRunnable nativeRunnable =
                        new NativeFTGSRunnable(readers, nativeCaches, onlyBinaryMetrics,
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        }

        // now reset the barrier value (yes, every thread will do it)
        writeFTGSSplitBarrier.set(null);
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
    protected ImhotepRemoteSession createImhotepRemoteSession(InetSocketAddress address,
                                                              String sessionId,
                                                              AtomicLong tempFileSizeBytesLeft) {
        return new ImhotepRemoteSession(address.getHostName(), address.getPort(),
                                        sessionId, tempFileSizeBytesLeft, useNativeFtgs);
    }

    private static class NativeFTGSRunnable {

        /* !@# Note that mappedFiles/mappedPtrs are maintained as parallel
            arrays because accessing a map from within JNI would be painful to
            say the least. */

        final String[] shardDirs;
        final String[] mappedFiles;
        final long[]   mappedPtrs;
        final long[]   packedTablePtrs;
        final boolean  onlyBinaryMetrics;
        final String[] intFields;
        final String[] stringFields;
        final String   splitsDir;
        final int      numGroups;
        final int      numStats;
        final int      numSplits;
        final int      numWorkers;
        final int[]    socketFDs;

        private Map<String, Long> combine(final FlamdexReader[] readers) {
            final Map<String, Long> result = Maps.newHashMap();
            for (final FlamdexReader reader: readers) {
                if (reader instanceof HasMapCache) {
                    ((HasMapCache) reader).getMapCache().getAddresses(result);
                }
            }
            return result;
        }

        private void flatten(final Map<String, Long> mapCaches) {
            int idx = 0;
            for (Map.Entry<String, Long> entry: mapCaches.entrySet()) {
                this.mappedFiles[idx] = entry.getKey();
                this.mappedPtrs[idx]  = entry.getValue().longValue();
                ++idx;
            }
        }

        NativeFTGSRunnable(final FlamdexReader[] readers,
                           final MultiCache[]    nativeCaches,
                           final boolean         onlyBinaryMetrics,
                           final String[]        intFields,
                           final String[]        stringFields,
                           final int             numGroups,
                           final int             numStats,
                           final int             numSplits,
                           final Socket[]        sockets) {

            this.shardDirs = getShardDirs(readers);

            this.packedTablePtrs = new long[nativeCaches.length];
            for (int index = 0; index < nativeCaches.length; ++index) {
                this.packedTablePtrs[index] = nativeCaches[index].getNativeAddress();
            }

            Map<String, Long> mapCaches = combine(readers);
            this.mappedFiles = new String[mapCaches.size()];
            this.mappedPtrs  = new long[mapCaches.size()];
            flatten(mapCaches);

            this.onlyBinaryMetrics = onlyBinaryMetrics;
            this.intFields         = intFields;
            this.stringFields      = stringFields;
            this.splitsDir         = System.getProperty("java.io.tmpdir", "/dev/null");
            this.numGroups         = numGroups;
            this.numStats          = numStats;
            this.numSplits         = numSplits;

            final String numWorkersStr = System.getProperty("imhotep.ftgs.num.workers", "8");
            final int    numWorkers    = Integer.parseInt(numWorkersStr);
            this.numWorkers         = numWorkers;

            java.util.ArrayList<Integer> socketFDArray = new java.util.ArrayList<Integer>();
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

        private String[] getShardDirs(final FlamdexReader[] readers) {
            final String[] shardDirs = new String[readers.length];

            class LoadDirThread extends Thread {
                final int index;
                LoadDirThread(final int index) { this.index = index; }
                public void run() {
                    String shardDir = readers[index].getDirectory();
                    try {
                        CachedFile cf        = CachedFile.create(shardDir);
                        File       cachedDir = cf.loadDirectory();
                        synchronized (shardDirs) {
                            shardDirs[index] = cachedDir.getAbsolutePath();
                        }
                    }
                    catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }

            LoadDirThread[] threads = new LoadDirThread[readers.length];
            try {
                for (int index = 0; index < readers.length; ++index) {
                    threads[index] = new LoadDirThread(index);
                    threads[index].start();
                }
            }
            finally {
                for (int index = 0; index < readers.length; ++index) {
                    try {
                        threads[index].join();
                    }
                    catch (InterruptedException ex) {
                        // !@# handle more gracefully
                    }
                }
            }
            return shardDirs;
        }
    }
}
