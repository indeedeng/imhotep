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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.imhotep.CachedMemoryReserver;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ImhotepMemoryCache;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.MetricKey;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.local.ImhotepNativeLocalSession;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.varexport.Export;
import com.indeed.util.varexport.VarExporter;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

    private final ScheduledExecutorService shardReload;
    private final ScheduledExecutorService shardStoreSync;
    private final ScheduledExecutorService heartBeat;
    private final Path shardsDir;
    private final Path shardTempDir;
    private final Path shardStoreDir;

    private boolean cleanupShardStoreDir = false; // 'true' only in test codepaths

    private final MemoryReserver memory;
    private final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache;

    private final FlamdexReaderSource flamdexReaderFactory;

    private final ShardUpdateListenerIf shardUpdateListener;

    private final ShardStore shardStore;

    private final AtomicReference<ShardMap>        shardMap    = new AtomicReference<>();
    private final AtomicReference<ShardInfoList>   shardList   = new AtomicReference<>();
    private final AtomicReference<DatasetInfoList> datasetList = new AtomicReference<>();

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
     * @param shardUpdateListener
     *            provides notification when shard/dataset lists change
     * @throws IOException
     *             if something bad happens
     */
    public LocalImhotepServiceCore(@Nullable Path shardsDir,
                                   @Nullable Path shardTempDir,
                                   @Nullable Path shardStoreDir,
                                   long memoryCapacity,
                                   boolean useCache,
                                   FlamdexReaderSource flamdexReaderFactory,
                                   LocalImhotepServiceConfig config,
                                   ShardUpdateListenerIf shardUpdateListener)
        throws IOException {
        this.shardsDir = shardsDir;
        this.shardUpdateListener = shardUpdateListener;

        /* check if the temp dir exists, try to create it if it does not */
        Preconditions.checkNotNull(shardTempDir, "shardTempDir is invalid");

        if (Files.exists(shardTempDir) && !Files.isDirectory(shardTempDir)) {
            throw new FileNotFoundException(shardTempDir + " is not a directory.");
        }
        if (Files.notExists(shardTempDir)) {
            Files.createDirectories(shardTempDir);
        }
        this.shardTempDir = shardTempDir;

        this.flamdexReaderFactory = flamdexReaderFactory;
        if (useCache) {
            freeCache = new ImhotepMemoryCache<MetricKey, IntValueLookup>();
            memory = new CachedMemoryReserver(new ImhotepMemoryPool(memoryCapacity), freeCache);
        } else {
            freeCache = null;
            memory = new ImhotepMemoryPool(memoryCapacity);
        }

        sessionManager = new LocalSessionManager();

        clearTempDir(shardTempDir);

        this.shardStoreDir = shardStoreDir;
        this.shardStore    = loadOrCreateShardStore(shardStoreDir);

        if (shardsDir != null) {
            final ShardDirIterator shardDirIterator = config.getShardDirIteratorFactory().get(shardsDir);

            ShardMap newShardMap = (shardStore != null) ?
                    new ShardMap(shardStore, shardsDir, memory, flamdexReaderFactory, freeCache) :
                    new ShardMap(memory, flamdexReaderFactory, freeCache);

            /* An empty ShardMap suggests that ShardStore had not been
             * initialized, so fallback to a synchronous directory scan. */
            if (newShardMap.isEmpty()) {
                log.info("Could not load ShardMap from cache: " + shardStoreDir + ". " +
                         "Scanning shard path instead.");
                newShardMap = new ShardMap(newShardMap, shardDirIterator);
                newShardMap.sync(shardStore);
                setShardMap(newShardMap, ShardUpdateListenerIf.Source.FILESYSTEM);
                log.info("Loaded ShardMap from filesystem: " + shardsDir);
            }
            else {
                setShardMap(newShardMap, ShardUpdateListenerIf.Source.CACHE);
                log.info("Loaded ShardMap from cache: " + shardStoreDir);
            }

            this.shardReload =
                    newFixedRateExecutor(new ShardReloader(shardDirIterator), config.getUpdateShardsFrequencySeconds());
        } else {
            this.shardReload = null;
        }

        /* TODO(johnf): consider pinning these threads... */
        this.shardStoreSync =
            newFixedRateExecutor(new ShardStoreSyncer(), config.getSyncShardStoreFrequencySeconds());
        this.heartBeat
            = newFixedRateExecutor(new HeartBeatChecker(), config.getHeartBeatCheckFrequencySeconds());

        VarExporter.forNamespace(getClass().getSimpleName()).includeInGlobal().export(this, "");
    }

    public LocalImhotepServiceCore(@Nullable Path shardsDir,
                                   @Nullable Path shardTempDir,
                                   long memoryCapacity,
                                   boolean useCache,
                                   FlamdexReaderSource flamdexReaderFactory,
                                   LocalImhotepServiceConfig config,
                                   ShardUpdateListenerIf shardUpdateListener)
        throws IOException {
        this(shardsDir, shardTempDir, Paths.get(System.getProperty("imhotep.shard.store")),
             memoryCapacity, useCache, flamdexReaderFactory, config, shardUpdateListener);
    }

    @VisibleForTesting
    public LocalImhotepServiceCore(@Nullable Path shardsDir,
                                   @Nullable Path shardTempDir,
                                   long memoryCapacity,
                                   boolean useCache,
                                   FlamdexReaderSource flamdexReaderFactory,
                                   LocalImhotepServiceConfig config)
        throws IOException {
        this(shardsDir, shardTempDir, Files.createTempDirectory("delete.me-imhotep.shard.store"),
             memoryCapacity, useCache, flamdexReaderFactory, config,
             new ShardUpdateListenerIf() {
                 public void onShardUpdate(final List<ShardInfo> shardList,
                                           final ShardUpdateListenerIf.Source source)
                 { }
                 public void onDatasetUpdate(final List<DatasetInfo> datasetList,
                                             final ShardUpdateListenerIf.Source source)
                 { }
             });
        cleanupShardStoreDir = true;
    }

    private ScheduledExecutorService newFixedRateExecutor(final Runnable runnable,
                                                          final int      freqSeconds) {
        final String name = runnable.getClass().getSimpleName() + "Thread";
        ScheduledExecutorService result =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        final Thread thread = new Thread(r, name);
                        thread.setDaemon(true);
                        return thread;
                    }
                });
        result.scheduleAtFixedRate(runnable, freqSeconds, freqSeconds, TimeUnit.SECONDS);
        return result;
    }

    private class ShardReloader implements Runnable {
        private final ShardDirIterator shardDirIterator;

        ShardReloader(final ShardDirIterator shardDirIterator) {
            this.shardDirIterator = shardDirIterator;
        }

        @Override
        public void run() {
            try {
                final ShardMap newShardMap = new ShardMap(shardMap.get(), shardDirIterator);
                setShardMap(newShardMap, ShardUpdateListenerIf.Source.FILESYSTEM);
            }
            catch (IOException e) {
                log.error("error updating shards", e);
            }
            catch (RuntimeException e) {
                log.error("error updating shards", e);
            }
        }
    }

    private class ShardStoreSyncer implements Runnable {
        @Override
        public void run() {
            try {
                final ShardMap currentShardMap = shardMap.get();
                currentShardMap.sync(shardStore);
            }
            catch (IOException e) {
                log.warn("error syncing shard store", e);
            }
            catch (RuntimeException e) {
                log.warn("error syncing shard store", e);
            }
        }
    }

    private class HeartBeatChecker implements Runnable {
        @Override
        public void run() {
            final long currentTime = System.currentTimeMillis();
            final Map<String, SessionManager.LastActionTimeLimit> lastActionTimes = getSessionManager().getLastActionTimes();
            final List<String> sessionsToClose = new ArrayList<String>();
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

    private void setShardMap(final ShardMap                     newShardMap,
                             final ShardUpdateListenerIf.Source source) {
        shardMap.set(newShardMap);
        try {
            shardList.set(new ShardInfoList(this.shardMap.get()));
            shardUpdateListener.onShardUpdate(this.shardList.get(), source);
        }
        catch (IOException ex) {
            log.error("could not build ShardInfoList", ex);
        }
        datasetList.set(new DatasetInfoList(this.shardMap.get()));
        shardUpdateListener.onDatasetUpdate(this.datasetList.get(), source);
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

    private void clearTempDir(Path directory) throws IOException {
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

    @Override public List<ShardInfo>     handleGetShardList() { return shardList.get();   }
    @Override public List<DatasetInfo> handleGetDatasetList() { return datasetList.get(); }

    @Override
    public ImhotepStatusDump handleGetStatusDump(boolean includeShardList) {
        final long usedMemory = memory.usedMemory();
        final long totalMemory = memory.totalMemory();

        final List<ImhotepStatusDump.SessionDump> openSessions =
                getSessionManager().getSessionDump();

        final List<ImhotepStatusDump.ShardDump> shards;
        if (includeShardList) {
            try {
                shards = shardMap.get().getShardDump();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        } else {
            shards = Collections.emptyList();
        }
        final int shardCount = shardMap.get().size();
        return new ImhotepStatusDump(usedMemory, totalMemory, openSessions, shards, shardCount);
    }

    @Override
    public List<String> getShardIdsForSession(String sessionId) {
        return getSessionManager().getShardIdsForSession(sessionId);
    }

    @Override
    public String handleOpenSession(final String dataset,
                                    final List<String> shardRequestList,
                                    String username,
                                    String clientName,
                                    final String ipAddress,
                                    final int clientVersion,
                                    final int mergeThreadLimit,
                                    final boolean optimizeGroupZeroLookups,
                                    String sessionId,
                                    AtomicLong tempFileSizeBytesLeft,
                                    final boolean useNativeFtgs,
                                    final long sessionTimeout)
        throws ImhotepOutOfMemoryException {

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
            final ShardMap.FlamdexReaderMap flamdexReaders =
                shardMap.get().getFlamdexReaders(dataset, shardRequestList);

            final Map<ShardId, CachedFlamdexReaderReference> flamdexes = Maps.newHashMap();
            final SessionObserver observer =
                new SessionObserver(dataset, sessionId, username);
            for (int i = 0; i < shardRequestList.size(); ++i) {
                final String shardId = shardRequestList.get(i);
                final Pair<ShardId, CachedFlamdexReaderReference> pair =
                        flamdexReaders.get(shardId);
                final CachedFlamdexReaderReference cachedFlamdexReaderReference =
                    pair.getSecond();
                try {
                    flamdexes.put(pair.getFirst(), cachedFlamdexReaderReference);
                    localSessions[i] = useNativeFtgs && flamdexReaders.allFlamdexReaders ?
                        new ImhotepNativeLocalSession(cachedFlamdexReaderReference,
                                                      new MemoryReservationContext(multiSessionMemoryContext),
                                                      tempFileSizeBytesLeft) :
                        new ImhotepJavaLocalSession(cachedFlamdexReaderReference,
                                                    this.shardTempDir.toString(),
                                                    new MemoryReservationContext(multiSessionMemoryContext),
                                                    tempFileSizeBytesLeft);
                    localSessions[i].addObserver(observer);
                } catch (RuntimeException | ImhotepOutOfMemoryException e) {
                    Closeables2.closeQuietly(cachedFlamdexReaderReference, log);
                    localSessions[i] = null;
                    throw e;
                }
            }

            final MTImhotepLocalMultiSession session =
                new MTImhotepLocalMultiSession(localSessions,
                                               new MemoryReservationContext(multiSessionMemoryContext),
                                               tempFileSizeBytesLeft,
                                               useNativeFtgs && flamdexReaders.allFlamdexReaders);
            getSessionManager().addSession(sessionId, session, flamdexes, username, clientName,
                                           ipAddress, clientVersion, dataset, sessionTimeout, multiSessionMemoryContext);
            session.addObserver(observer);
        }
        catch (IOException ex) {
            Throwables.propagate(ex);
        }
        catch (RuntimeException ex) {
            closeNonNullSessions(localSessions);
            throw ex;
        }
        catch (ImhotepOutOfMemoryException ex) {
            closeNonNullSessions(localSessions);
            throw ex;
        }

        return sessionId;
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
        if (shardReload != null) {
            // could be null if shardDir is missing
            shardReload.shutdown();
        }
        shardStoreSync.shutdown();
        heartBeat.shutdown();
        if (shardStore != null) {
            try {
                shardStore.close();
            }
            catch (IOException ex) {
                log.warn("failed to close ShardStore", ex);
            }
        }
        if (cleanupShardStoreDir) {
            try {
                ShardStore.deleteExisting(shardStoreDir);
            }
            catch (IOException ex) {
                log.warn("failed to clean up ShardStore: " + shardStoreDir, ex);
            }
        }
    }

    @Export(name = "loaded-shard-count",
            doc = "number of loaded shards for each dataset",
            expand = true)
    public Map<String, Integer> getLoadedShardCount() {
        return shardMap.get().getShardCounts();
    }

    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    private String generateSessionId() {
        final int currentCounter = counter.getAndIncrement();
        return new StringBuilder(24).append(toHexString(System.currentTimeMillis()))
                                    .append(toHexString(currentCounter)).toString();
    }

    private static String toHexString(long l) {
        final StringBuilder sb = new StringBuilder(16);
        for (int i = 0; i < 16; ++i) {
            final int nibble = (int) ((l >>> ((15 - i) * 4)) & 0x0F);
            sb.append((char) (nibble < 10 ? '0' + nibble : 'a' + nibble - 10));
        }
        return sb.toString();
    }

    private static String toHexString(int x) {
        final StringBuilder sb = new StringBuilder(8);
        for (int i = 0; i < 8; ++i) {
            final int nibble = (x >>> ((7 - i) * 4)) & 0x0F;
            sb.append((char) (nibble < 10 ? '0' + nibble : 'a' + nibble - 10));
        }
        return sb.toString();
    }
}
