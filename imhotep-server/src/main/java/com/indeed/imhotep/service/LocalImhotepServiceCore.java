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
 package com.indeed.imhotep.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.reader.FlamdexFormatVersion;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.reader.GenericFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.imhotep.*;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.imhotep.protobuf.ShardNameNumDocsPair;
import com.indeed.imhotep.scheduling.SchedulerType;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.varexport.Export;
import com.indeed.util.varexport.VarExporter;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jsgroth
 */
public class LocalImhotepServiceCore
    extends AbstractImhotepServiceCore {
    private static final Logger log = Logger.getLogger(LocalImhotepServiceCore.class);
    private static final DateTimeZone ZONE = DateTimeZone.forOffsetHours(-6);

    private final LocalSessionManager sessionManager;

    private final ScheduledExecutorService heartBeat;
    private final Path shardTempDir;


    private final MemoryReserver memory;

    /**
     * @param shardsDir
     *            root directory from which to read shards
     * @param shardTempDir
     *            root directory for the daemon scratch area
     * @param shardStoreDir
     *            root directory of the [Shard|Dataset]Info cache (ShardStore).
     * @param memoryCapacity
     *            the capacity in bytes allowed to be allocated for large
     *            int/long arrays
     * @param flamdexReaderFactory
     *            the factory to use for opening FlamdexReaders
     * @param config
     *            additional config parameters
     * @param statsEmitter
     *            allows sending stats about service activity
     * @throws IOException
     *             if something bad happens
     */
    public LocalImhotepServiceCore(@Nullable final Path shardsDir,
                                   @Nullable final Path shardTempDir,
                                   @Nullable final Path shardStoreDir,
                                   final long memoryCapacity,
                                   final FlamdexReaderSource flamdexReaderFactory,
                                   final LocalImhotepServiceConfig config,
                                   final MetricStatsEmitter statsEmitter)
        throws IOException {

        /* check if the temp dir exists, try to create it if it does not */
        Preconditions.checkNotNull(shardTempDir, "shardTempDir is invalid");

        if (Files.exists(shardTempDir) && !Files.isDirectory(shardTempDir)) {
            throw new FileNotFoundException(shardTempDir + " is not a directory.");
        }
        if (Files.notExists(shardTempDir)) {
            Files.createDirectories(shardTempDir);
        }
        this.shardTempDir = shardTempDir;

        memory = new ImhotepMemoryPool(memoryCapacity);
        if(config.getCpuSlots() > 0) {
            TaskScheduler.CPUScheduler = new TaskScheduler(config.getCpuSlots(),
                    TimeUnit.SECONDS.toNanos(config.getCpuSchedulerHistoryLengthSeconds()),
                    TimeUnit.SECONDS.toNanos(1), SchedulerType.CPU,
                    statsEmitter);
        }

        if(config.getRemoteFSIOSlots() > 0) {
            TaskScheduler.RemoteFSIOScheduler = new TaskScheduler(config.getRemoteFSIOSlots(),
                    TimeUnit.SECONDS.toNanos(config.getRemoteFSIOSchedulerHistoryLengthSeconds()),
                    TimeUnit.SECONDS.toNanos(1), SchedulerType.REMOTE_FS_IO,
                    statsEmitter);
        }

        sessionManager = new LocalSessionManager(statsEmitter, config.getMaxSessionsTotal(), config.getMaxSessionsPerUser());

        clearTempDir(shardTempDir);


        /* TODO(johnf): consider pinning these threads... */
        this.heartBeat
            = newFixedRateExecutor(new HeartBeatChecker(), config.getHeartBeatCheckFrequencySeconds());

        VarExporter.forNamespace(getClass().getSimpleName()).includeInGlobal().export(this, "");
    }

    public LocalImhotepServiceCore(@Nullable final Path shardsDir,
                                   @Nullable final Path shardTempDir,
                                   final long memoryCapacity,
                                   final FlamdexReaderSource flamdexReaderFactory,
                                   final LocalImhotepServiceConfig config,
                                   final ShardUpdateListenerIf shardUpdateListener)
        throws IOException {
        this(shardsDir, shardTempDir, Paths.get(System.getProperty("imhotep.shard.store")),
             memoryCapacity, flamdexReaderFactory, config, config.getStatsEmitter());
    }

    @VisibleForTesting
    public LocalImhotepServiceCore(@Nullable final Path shardsDir,
                                   @Nullable final Path shardTempDir,
                                   final long memoryCapacity,
                                   final FlamdexReaderSource flamdexReaderFactory,
                                   final LocalImhotepServiceConfig config)
        throws IOException {
        this(shardsDir, shardTempDir, Files.createTempDirectory("delete.me-imhotep.shard.store"),
             memoryCapacity, flamdexReaderFactory, config,
             MetricStatsEmitter.NULL_EMITTER
        );
    }

    private ScheduledExecutorService newFixedRateExecutor(final Runnable runnable,
                                                          final int      freqSeconds) {
        final String name = runnable.getClass().getSimpleName() + "Thread";
        final ScheduledExecutorService result =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(@Nonnull final Runnable r) {
                        final Thread thread = new Thread(r, name);
                        thread.setDaemon(true);
                        return thread;
                    }
                });
        result.scheduleAtFixedRate(runnable, freqSeconds, freqSeconds, TimeUnit.SECONDS);
        return result;
    }

    private class HeartBeatChecker implements Runnable {
        @Override
        public void run() {
            final long currentTime = System.currentTimeMillis();
            final Map<String, SessionManager.LastActionTimeLimit> lastActionTimes = getSessionManager().getLastActionTimes();
            final List<String> sessionsToClose = new ArrayList<>();
            for (final String sessionId : lastActionTimes.keySet()) {
                final SessionManager.LastActionTimeLimit lastActionTimeLimit = lastActionTimes.get(sessionId);
                if (lastActionTimeLimit.getLastActionTime() < currentTime - lastActionTimeLimit.getSessionTimeoutDuration()) {
                    sessionsToClose.add(sessionId);
                }
            }
            for (final String sessionId : sessionsToClose) {
                getSessionManager().removeAndCloseIfExists(sessionId,
                                                           new TimeoutException("Session timed out."));
            }
        }
    }

    @Override
    protected LocalSessionManager getSessionManager() {
        return sessionManager;
    }

    private static ShardStore loadOrCreateShardStore(@Nullable final Path shardStoreDir) {

        ShardStore result = null;

        if (shardStoreDir == null) {
            log.error("shardStoreDir is null; did you set imhotep.shard.store?");
            return null;
        }

        try {
            result = new ShardStore(shardStoreDir);
        }
        catch (final Exception ex1) {
            log.error("unable to create/load ShardStore: " + shardStoreDir +
                      " will attempt to repair", ex1);
            try {
                ShardStore.deleteExisting(shardStoreDir);
                result = new ShardStore(shardStoreDir);
            }
            catch (final Exception ex2) {
                log.error("failed to cleanup and recreate ShardStore: " + shardStoreDir +
                          " operator assistance is required", ex2);
            }
        }
        return result;
    }

    private void clearTempDir(final Path directory) throws IOException {
        if (Files.notExists(directory)) {
            throw new IOException(directory + " does not exist.");
        }
        if (!Files.isDirectory(directory)) {
            throw new IOException(directory + " is not a directory.");
        }

        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            for (final Path p : dirStream) {
                final String baseName = p.getFileName().toString();
                final boolean isDirectory = Files.isDirectory(p);

                if (isDirectory && baseName.endsWith(".optimization_log")) {
                    /* an optimized index */
                    PosixFileOperations.rmrf(p);
                }
                if (!isDirectory && baseName.startsWith(".tmp")) {
                    /* an optimization log */
                    Files.delete(p);
                }
                if (!isDirectory && baseName.startsWith("ftgs") && baseName.endsWith(".tmp")) {
                    /* created by AbstractImhotepMultisession::persist() */
                    Files.delete(p);
                }
                if (!isDirectory && baseName.startsWith("native-split")) {
                    /* a temporary split file created by native code (see
                     * shard.cpp, Shard::split_filename()) */
                    Files.delete(p);
                }
            }
        }
    }

    @Override
    public ImhotepStatusDump handleGetStatusDump(final boolean includeShardList) {
        final long usedMemory = memory.usedMemory();
        final long totalMemory = memory.totalMemory();

        final List<ImhotepStatusDump.SessionDump> openSessions =
                getSessionManager().getSessionDump();
        return new ImhotepStatusDump(usedMemory, totalMemory, openSessions);
    }

    @Override
    public List<String> getShardIdsForSession(final String sessionId) {
        return getSessionManager().getShardIdsForSession(sessionId);
    }

    @Override
    public String handleOpenSession(String dataset, List<ShardNameNumDocsPair> shardRequestList, String username, String clientName, String ipAddress, int clientVersion, int mergeThreadLimit, boolean optimizeGroupZeroLookups, String sessionId, AtomicLong tempFileSizeBytesLeft, long sessionTimeout, List<Integer> numDocs) throws ImhotepOutOfMemoryException {
        if (Strings.isNullOrEmpty(sessionId)) {
            sessionId = generateSessionId();
        }

        if (Strings.isNullOrEmpty(clientName)) {
            // infer clientName from username for old clients
            if (username.indexOf(':') > 0) {
                final String[] usernameParts = username.split(":", -1);
                clientName = usernameParts[0];
                username = usernameParts[1];
            } else {
                clientName = username;
            }
        }

        final ImhotepLocalSession[] localSessions =
                new ImhotepLocalSession[shardRequestList.size()];

        final MemoryReservationContext multiSessionMemoryContext = new MemoryReservationContext(memory);


        try {
            final Map<ShardId, FlamdexReader> flamdexes = Maps.newHashMap();
            final SessionObserver observer =
                    new SessionObserver(dataset, sessionId, username, clientName, ipAddress);
            for (int i = 0; i < shardRequestList.size(); ++i) {
                final ShardDir shardDir = new ShardDir(Paths.get(dataset, shardRequestList.get(i).getShardName()));
                final ShardId shardId = new ShardId(dataset, shardDir.getId(), shardDir.getVersion(), shardDir.getIndexDir());
                final FlamdexReader reader;
                final RemoteCachingPath datasetsDir = (RemoteCachingPath) Paths.get(RemoteCachingFileSystemProvider.URI);
                RemoteCachingPath path = datasetsDir.resolve(shardDir.getIndexDir()+".sqar");

                if(numDocs != null) {
                    reader = SimpleFlamdexReader.open(path, numDocs.get(i));
                } else {
                    reader = SimpleFlamdexReader.open(path);
                }
                try {
                    flamdexes.put(shardId, reader);
                    localSessions[i] =
                            new ImhotepJavaLocalSession(sessionId,
                                    reader,
                                    this.shardTempDir.toString(),
                                    new MemoryReservationContext(multiSessionMemoryContext),
                                    tempFileSizeBytesLeft);
                    localSessions[i].addObserver(observer);
                } catch (RuntimeException | ImhotepOutOfMemoryException e) {
                    Closeables2.closeQuietly(reader, log);
                    localSessions[i] = null;
                    throw e;
                }
            }

            final MTImhotepLocalMultiSession session = new MTImhotepLocalMultiSession(
                    sessionId,
                    localSessions,
                    new MemoryReservationContext(multiSessionMemoryContext),
                    tempFileSizeBytesLeft,
                    username,
                    clientName);
            getSessionManager().
                    addSession(sessionId, session, flamdexes, username, clientName,
                            ipAddress, clientVersion, dataset, sessionTimeout, multiSessionMemoryContext);
            session.addObserver(observer);
        }
        catch (final IOException ex) {
            Throwables.propagate(ex);
        }
        catch (final RuntimeException | ImhotepOutOfMemoryException ex) {
            closeNonNullSessions(localSessions);
            throw ex;
        }

        return sessionId;
    }

    @Override
    @Deprecated
    public String handleOpenSession(final String dataset,
                                    final List<ShardNameNumDocsPair> shardRequestList,
                                    String username,
                                    String clientName,
                                    final String ipAddress,
                                    final int clientVersion,
                                    final int mergeThreadLimit,
                                    final boolean optimizeGroupZeroLookups,
                                    String sessionId,
                                    final AtomicLong tempFileSizeBytesLeft,
                                    final long sessionTimeout)
        throws ImhotepOutOfMemoryException {
        return handleOpenSession(dataset, shardRequestList, username, clientName, ipAddress, clientVersion, mergeThreadLimit, optimizeGroupZeroLookups, sessionId, tempFileSizeBytesLeft, sessionTimeout, null);
    }

    private static void closeNonNullSessions(final ImhotepSession[] sessions) {
        for (final ImhotepSession session : sessions) {
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    public void close() {
        super.close();
        heartBeat.shutdown();
        TaskScheduler.CPUScheduler.close();
        TaskScheduler.RemoteFSIOScheduler.close();
    }

    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    private String generateSessionId() {
        final int currentCounter = counter.getAndIncrement();
        return new StringBuilder(24).append(toHexString(System.currentTimeMillis()))
                                    .append(toHexString(currentCounter)).toString();
    }

    private static String toHexString(final long l) {
        final StringBuilder sb = new StringBuilder(16);
        for (int i = 0; i < 16; ++i) {
            final int nibble = (int) ((l >>> ((15 - i) * 4)) & 0x0F);
            sb.append((char) (nibble < 10 ? '0' + nibble : 'a' + nibble - 10));
        }
        return sb.toString();
    }

    private static String toHexString(final int x) {
        final StringBuilder sb = new StringBuilder(8);
        for (int i = 0; i < 8; ++i) {
            final int nibble = (x >>> ((7 - i) * 4)) & 0x0F;
            sb.append((char) (nibble < 10 ? '0' + nibble : 'a' + nibble - 10));
        }
        return sb.toString();
    }
}
