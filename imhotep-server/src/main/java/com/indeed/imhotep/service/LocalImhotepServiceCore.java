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
import com.google.common.collect.Maps;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.imhotep.*;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.imhotep.protobuf.ShardNameNumDocsPair;
import com.indeed.imhotep.scheduling.SchedulerType;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.varexport.VarExporter;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

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

    private final LocalSessionManager sessionManager;

    private final ScheduledExecutorService heartBeat;
    private final Path shardTempDir;


    private final MemoryReserver memory;

    /**
     * @param shardTempDir
     *            root directory for the daemon scratch area
     * @param memoryCapacity
     *            the capacity in bytes allowed to be allocated for large
     *            int/long arrays
     * @param config
     *            additional config parameters
     * @param statsEmitter
     *            allows sending stats about service activity
     * @throws IOException
     *             if something bad happens
     */
    public LocalImhotepServiceCore(@Nullable final Path shardTempDir,
                                   final long memoryCapacity,
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

    @VisibleForTesting
    public LocalImhotepServiceCore(@Nullable final Path shardTempDir,
                                   final long memoryCapacity,
                                   final LocalImhotepServiceConfig config)
        throws IOException {
        this(shardTempDir,
             memoryCapacity, config,
             MetricStatsEmitter.NULL_EMITTER
        );
    }

    private ScheduledExecutorService newFixedRateExecutor(final Runnable runnable,
                                                          final int      freqSeconds) {
        final String name = runnable.getClass().getSimpleName() + "Thread";
        final ScheduledExecutorService result =
            Executors.newSingleThreadScheduledExecutor(r -> {
                final Thread thread = new Thread(r, name);
                thread.setDaemon(true);
                return thread;
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
    public String handleOpenSession(String dataset, List<ShardNameNumDocsPair> shardRequestList, String username, String clientName, String ipAddress, int clientVersion, int mergeThreadLimit, boolean optimizeGroupZeroLookups, String sessionId, AtomicLong tempFileSizeBytesLeft, long sessionTimeout) throws ImhotepOutOfMemoryException {
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
                final int numDocs = shardRequestList.get(i).getNumDocs();
                final FlamdexReader reader;
                final RemoteCachingPath datasetsDir = (RemoteCachingPath) Paths.get(RemoteCachingFileSystemProvider.URI);
                RemoteCachingPath path = datasetsDir.resolve(shardDir.getIndexDir().toString());
                reader = SimpleFlamdexReader.open(path, numDocs);

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
            throw Throwables.propagate(ex);
        }
        catch (final RuntimeException | ImhotepOutOfMemoryException ex) {
            Closeables2.closeAll(log, localSessions);
            throw ex;
        }

        return sessionId;
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
        return toHexString(System.currentTimeMillis()) +
                toHexString(currentCounter);
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
