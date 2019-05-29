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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.SlotTiming;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.imhotep.protobuf.FileAttributesMessage;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.protobuf.ShardBasicInfoMessage;
import com.indeed.imhotep.scheduling.SchedulerType;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.varexport.VarExporter;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.WillNotClose;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.indeed.imhotep.utils.ImhotepResponseUtils.appendErrorMessage;
import static com.indeed.imhotep.utils.ImhotepResponseUtils.newErrorResponse;

/**
 * @author jsgroth
 */
public class LocalImhotepServiceCore
    extends AbstractImhotepServiceCore {
    private static final Logger log = Logger.getLogger(LocalImhotepServiceCore.class);

    private final LocalSessionManager sessionManager;

    private final ScheduledExecutorService heartBeat;
    private final Path shardTempDir;
    private final Path rootDir;

    private final Host myHost;
    private final MemoryReserver memory;
    private final ConcurrentFlamdexReaderFactory flamderReaderFactory;

    /**
     * @param shardTempDir
     *            root directory for the daemon scratch area
     * @param memoryCapacity
     *            the capacity in bytes allowed to be allocated for large
     *            int/long arrays
     * @param config
     *            additional config parameters
     * @throws IOException
     *             if something bad happens
     */
    public LocalImhotepServiceCore(@Nullable final Path shardTempDir,
                                   final long memoryCapacity,
                                   final FlamdexReaderSource flamdexReaderFactory,
                                   final LocalImhotepServiceConfig config,
                                   final Path rootDir,
                                   final Host myHost)
        throws IOException {

        this.rootDir = rootDir;
        final MetricStatsEmitter statsEmitter = config.getStatsEmitter();
        this.myHost = myHost;

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
        final FlamdexReaderSource factory;
        if(flamdexReaderFactory != null) {
            factory = flamdexReaderFactory;
        } else {
            factory = new GenericFlamdexReaderSource();
        }
        final ShardLocator shardLocator = ShardLocator.combine(
                config.getDynamicShardLocator(),
                config.areShardsSQARed() ? ShardLocator.appendingSQARShardLocator(rootDir, myHost) : ShardLocator.pathShardLocator(rootDir, myHost)
        );
        this.flamderReaderFactory = new ConcurrentFlamdexReaderFactory(memory, factory, shardLocator);

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
    public void handleGetAndSendShardFile(
            final String fileUri,
            final SlotTiming slotTiming,
            final ImhotepResponse.Builder builder,
            @WillNotClose final OutputStream os) throws IOException {
        final Path path;
        // getShardFilePath explains why handling NoSuchFileException locally rather than throwing it out
        try {
            path = getShardFilePath(fileUri);
        } catch (final NoSuchFileException e) {
            log.debug("sending response");
            ImhotepProtobufShipping.sendProtobuf(newErrorResponse(e), os);
            log.debug("response sent");
            return;
        }

        builder.setFileLength(Files.size(path));
        builder.setSlotTiming(slotTiming.writeToSlotTimingMessage());
        log.debug("sending shard file response");
        ImhotepProtobufShipping.sendProtobufNoFlush(builder.build(), os);
        try (final InputStream is = Files.newInputStream(path)) {
            IOUtils.copy(is, os);
        }
        os.flush();
        log.debug("shard file response sent");
    }

    @Override
    public void handleListShardDirRecursively(final String shardDirUri, final ImhotepResponse.Builder builder) throws IOException {
        final Path dirPath;
        try {
            dirPath = getShardFilePath(shardDirUri);
        } catch (final NoSuchFileException e) {
            appendErrorMessage(e, builder);
            return;
        }

        final List<FileAttributesMessage> attributeMessageList = Files.walk(dirPath).map(path -> {
            try {
                final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
                // only list files
                // don't call filter(Files::isRegularFile) to avoid readAttributes twice
                if (attributes.isDirectory()) {
                    return null;
                }
                return FileAttributesMessage.newBuilder()
                        .setPath(dirPath.relativize(path).toString())
                        .setSize(attributes.size())
                        .setIsDirectory(attributes.isDirectory())
                        .build();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        builder.addAllFilesAttributes(attributeMessageList);
    }

    private Path getShardFilePath(final String fileUri) throws IOException {
        final Path path = Paths.get(URI.create(fileUri));
        if (!(path instanceof RemoteCachingPath)) {
            throw new IllegalArgumentException("path is not a valid RemoteCachingPath, path = " + path);
        }

        // check if a file in sqar directory by RemoteCachingFileSystem will access a lot of non-existed files.
        // to avoid many useless exceptions, we have to catch and handle them locally
        if (!Files.exists(path)) {
            throw new NoSuchFileException(path.toString());
        }
        return path;
    }

    @Override
    public List<String> getShardsForSession(final String sessionId) {
        return getSessionManager().getShardsForSession(sessionId);
    }

    @Override
    public String handleOpenSession(
            String dataset,
            List<ShardBasicInfoMessage> shardRequestList,
            String username,
            String clientName,
            String ipAddress,
            byte priority,
            int clientVersion,
            int mergeThreadLimit,
            boolean optimizeGroupZeroLookups,
            String sessionId,
            AtomicLong tempFileSizeBytesLeft,
            long sessionTimeout) throws ImhotepOutOfMemoryException {
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
            // Construct flamdex readers
            final List<ConcurrentFlamdexReaderFactory.CreateRequest> readerRequests =
                    shardRequestListToFlamdexReaderRequests(dataset, shardRequestList, username, clientName, priority);
            final Map<Path, SharedReference<CachedFlamdexReader>> flamdexes = flamderReaderFactory.constructFlamdexReaders(readerRequests);

            // Construct local sessions using the above readers
            final SessionObserver observer = new SessionObserver(dataset, sessionId, username, clientName, ipAddress);
            int sessionIndex = 0;
            for (Map.Entry<Path, SharedReference<CachedFlamdexReader>> pathAndFlamdexReader : flamdexes.entrySet()) {
                final CachedFlamdexReaderReference flamdexForSession = new CachedFlamdexReaderReference(pathAndFlamdexReader.getValue());
                final ImhotepLocalSession localSession;
                try {
                    localSession = new ImhotepJavaLocalSession(sessionId,
                            flamdexForSession,
                            new ShardDir(ShardDir.cleanPath(pathAndFlamdexReader.getKey())).getTimeInterval(),
                            this.shardTempDir.toString(),
                            new MemoryReservationContext(multiSessionMemoryContext),
                            tempFileSizeBytesLeft);
                    localSession.addObserver(observer);
                } catch (RuntimeException | ImhotepOutOfMemoryException e) {
                    Closeables2.closeQuietly(flamdexForSession, log);
                    throw e;
                }
                localSessions[sessionIndex++] = localSession;
            }

            final MTImhotepLocalMultiSession session = new MTImhotepLocalMultiSession(
                    sessionId,
                    localSessions,
                    new MemoryReservationContext(multiSessionMemoryContext),
                    tempFileSizeBytesLeft,
                    username,
                    clientName,
                    priority);

            // create flamdex reference copies for the session manager
            final Map<Path, CachedFlamdexReaderReference> flamdexesForSessionManager = Maps.newHashMap();
            for (Map.Entry<Path, SharedReference<CachedFlamdexReader>> pathAndFlamdexReader : flamdexes.entrySet()) {
                flamdexesForSessionManager.put(pathAndFlamdexReader.getKey(), new CachedFlamdexReaderReference(pathAndFlamdexReader.getValue().copy()));
            }
            getSessionManager().
                    addSession(sessionId, session, flamdexesForSessionManager, username, clientName,
                            ipAddress, clientVersion, dataset, sessionTimeout, priority, multiSessionMemoryContext);
            session.addObserver(observer);
        }
        catch (final RuntimeException | ImhotepOutOfMemoryException ex) {
            Closeables2.closeAll(log, localSessions);
            throw ex;
        } catch (Exception e) {
            Closeables2.closeAll(log, localSessions);
            throw Throwables.propagate(e);
        }

        return sessionId;
    }

    private List<ConcurrentFlamdexReaderFactory.CreateRequest> shardRequestListToFlamdexReaderRequests(final String dataset,
                                                                                                       final List<ShardBasicInfoMessage> shardRequestList,
                                                                                                       final String userName,
                                                                                                       final String clientName,
                                                                                                       final byte priority) {
        final List<ConcurrentFlamdexReaderFactory.CreateRequest> readerRequests = Lists.newArrayList();
        for (final ShardBasicInfoMessage aShardRequestList : shardRequestList) {
            final String shardName = aShardRequestList.getShardName();
            final int numDocs = aShardRequestList.getNumDocs();
            Host shardOwner = null;
            if (aShardRequestList.hasShardOwner()) {
                final HostAndPort protoOwner = aShardRequestList.getShardOwner();
                shardOwner = new Host(protoOwner.getHost(), protoOwner.getPort());
            }
            final ShardHostInfo shardHostInfo = new ShardHostInfo(shardName, shardOwner);
            readerRequests.add(new ConcurrentFlamdexReaderFactory.CreateRequest(dataset, shardHostInfo, numDocs,
                    userName, clientName, priority));
        }
        return readerRequests;
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
