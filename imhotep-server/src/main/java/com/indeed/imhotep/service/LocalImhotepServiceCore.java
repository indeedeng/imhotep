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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.util.core.Pair;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.io.Files;
import com.indeed.util.core.reference.AtomicSharedReference;
import com.indeed.util.core.reference.ReloadableSharedReference;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.varexport.Export;
import com.indeed.util.varexport.VarExporter;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.RawFlamdexReader;
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
import com.indeed.imhotep.io.ReadLock;
import com.indeed.imhotep.io.Shard;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.imhotep.local.ImhotepNativeLocalSession;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jsgroth
 */
public class LocalImhotepServiceCore extends AbstractImhotepServiceCore {
    private static final Logger log = Logger.getLogger(LocalImhotepServiceCore.class);

    private static final Pattern VERSION_PATTERN = Pattern.compile("^(.+)\\.(\\d{14})$");

    private static final long SESSION_EXPIRATION_TIME_MILLIS = 30L * 60 * 1000;

    private final LocalSessionManager sessionManager;

    private final ExecutorService executor;

    private final ScheduledExecutorService shardReload;
    private final ScheduledExecutorService heartBeat;
    private final String shardsDirectory;
    private final String shardTempDirectory;

    private final MemoryReserver memory;
    private final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache;

    private final FlamdexReaderSource flamdexReaderFactory;

    // these maps will not be modified but the references will periodically be
    // swapped
    private volatile Map<String, Map<String, AtomicSharedReference<Shard>>> shards;
    private volatile List<ShardInfo> shardList;
    private volatile List<DatasetInfo> datasetList;

    private final Map<File, RandomAccessFile> lockFileMap = Maps.newHashMap();

    /**
     * @param shardsDirectory
     *            root directory from which to read shards
     * @param memoryCapacity
     *            the capacity in bytes allowed to be allocated for large
     *            int/long arrays
     * @param flamdexReaderFactory
     *            the factory to use for opening FlamdexReaders
     * @param config
     *            additional config parameters
     * @throws IOException
     *             if something bad happens
     */
    public LocalImhotepServiceCore(String shardsDirectory,
                                   String shardTempDir,
                                   long memoryCapacity,
                                   boolean useCache,
                                   FlamdexReaderSource flamdexReaderFactory,
                                   LocalImhotepServiceConfig config) throws IOException {
        this.shardsDirectory = shardsDirectory;

        /* check if the temp dir exists, try to create it if it does not */
        final File tempDir = new File(shardTempDir);
        if (tempDir.exists() && !tempDir.isDirectory()) {
            throw new FileNotFoundException(shardTempDir + " is not a directory.");
        }
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        this.shardTempDirectory = shardTempDir;

        this.flamdexReaderFactory = flamdexReaderFactory;
        if (useCache) {
            freeCache = new ImhotepMemoryCache<MetricKey, IntValueLookup>();
            memory = new CachedMemoryReserver(new ImhotepMemoryPool(memoryCapacity), freeCache);
        } else {
            freeCache = null;
            memory = new ImhotepMemoryPool(memoryCapacity);
        }

        sessionManager = new LocalSessionManager();
        /* allow temp dir to be null for testing */
        if (shardTempDir != null) {
            clearTempDir(shardTempDir);
        }
        updateShards();

        executor =
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true)
                                                                        .setNameFormat("LocalImhotepServiceCore-Worker-%d")
                                                                        .build());

        shardReload = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                final Thread thread = new Thread(r, "ShardReloaderThread");
                thread.setDaemon(true);
                return thread;
            }
        });

        heartBeat = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                final Thread thread = new Thread(r, "HeartBeatCheckerThread");
                thread.setDaemon(true);
                return thread;
            }
        });
        shardReload.scheduleAtFixedRate(new ShardReloader(),
                                        config.getUpdateShardsFrequencySeconds(),
                                        config.getUpdateShardsFrequencySeconds(),
                                        TimeUnit.SECONDS);
        heartBeat.scheduleAtFixedRate(new HeartBeatChecker(),
                                      config.getHeartBeatCheckFrequencySeconds(),
                                      config.getHeartBeatCheckFrequencySeconds(),
                                      TimeUnit.SECONDS);

        VarExporter.forNamespace(getClass().getSimpleName()).includeInGlobal().export(this, "");
    }

    private class ShardReloader implements Runnable {
        @Override
        public void run() {
            try {
                updateShards();
            } catch (RuntimeException e) {
                log.error("error updating shards", e);
            } catch (IOException e) {
                log.error("error updating shards", e);
            }
        }
    }

    private class HeartBeatChecker implements Runnable {
        @Override
        public void run() {
            final long minTime = System.currentTimeMillis() - SESSION_EXPIRATION_TIME_MILLIS;
            final Map<String, Long> lastActionTimes = getSessionManager().getLastActionTimes();
            final List<String> sessionsToClose = new ArrayList<String>();
            for (final String sessionId : lastActionTimes.keySet()) {
                final long lastActionTime = lastActionTimes.get(sessionId);
                if (lastActionTime < minTime) {
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

    private void clearTempDir(String directory) throws IOException {
        final File tmpDir = new File(directory);

        if (!tmpDir.exists()) {
            throw new IOException(directory + " does not exist.");
        }
        if (!tmpDir.isDirectory()) {
            throw new IOException(directory + " is not a directory.");
        }

        for (File f : tmpDir.listFiles()) {
            if (f.isDirectory() && f.getName().endsWith(".optimization_log")) {
                /* an optimized index */
                PosixFileOperations.rmrf(f);
            }
            if (!f.isDirectory() && f.getName().startsWith(".tmp")) {
                /* an optimization log */
                f.delete();
            }
        }

    }

    private void updateShards() throws IOException {
        final String canonicalShardsDirectory = Files.getCanonicalPath(shardsDirectory);
        if (canonicalShardsDirectory == null) {
            shards = Maps.newHashMap();
            shardList = Lists.newArrayList();
            datasetList = Lists.newArrayList();
            return;
        }

        Map<String, Map<String, AtomicSharedReference<Shard>>> oldShards = shards;
        if (oldShards == null) {
            oldShards = Maps.newHashMap();
        }

        final Map<String, Map<String, AtomicSharedReference<Shard>>> newShards = Maps.newHashMap();
        for (final File datasetDir : new File(canonicalShardsDirectory).listFiles()) {
            if (!datasetDir.isDirectory()) {
                continue;
            }

            final String dataset = datasetDir.getName();
            Map<String, AtomicSharedReference<Shard>> oldDatasetShards = oldShards.get(dataset);
            if (oldDatasetShards == null) {
                oldDatasetShards = Maps.newHashMap();
            }

            final Map<String, AtomicSharedReference<Shard>> newDatasetShards = Maps.newHashMap();

            for (final File shardDir : datasetDir.listFiles()) {
                if (!shardDir.isDirectory()) {
                    continue;
                }

                try {
                    final String shardId;
                    final long shardVersion;
                    final Matcher matcher = VERSION_PATTERN.matcher(shardDir.getName());
                    if (matcher.matches()) {
                        shardId = matcher.group(1);
                        shardVersion = Long.parseLong(matcher.group(2));
                    } else {
                        shardId = shardDir.getName();
                        shardVersion = 0L;
                    }

                    final String canonicalShardDir = shardDir.getCanonicalPath();
                    final ReadLock readLock;
                    try {
                        readLock = ReadLock.lock(lockFileMap, new File(canonicalShardDir));
                    } catch (ReadLock.AlreadyOpenException e) {
                        // already loaded
                        if (!newDatasetShards.containsKey(shardId)) {
                            if (!oldDatasetShards.containsKey(shardId)) {
                                log.error("shard " + shardId
                                        + " claims to be open but isn't referenced");
                            } else {
                                newDatasetShards.put(shardId, oldDatasetShards.get(shardId));
                            }
                        }
                        continue;
                    } catch (ReadLock.ShardDeletedException e) {
                        log.info("shard " + shardDir.getName() + " in dataset " + dataset
                                + " was deleted before read lock could be acquired");
                        continue;
                    } catch (ReadLock.LockAquisitionException e) {
                        log.error("could not lock directory " + canonicalShardDir, e);
                        continue;
                    }

                    final SharedReference<ReadLock> readLockRef = SharedReference.create(readLock);
                    final Shard newShard;

                    try {
                        final ReloadableSharedReference.Loader<CachedFlamdexReader, IOException> loader =
                                new ReloadableSharedReference.Loader<CachedFlamdexReader, IOException>() {
                                    @Override
                                    public CachedFlamdexReader load() throws IOException {
                                        final FlamdexReader flamdex =
                                                flamdexReaderFactory.openReader(canonicalShardDir);
                                        final SharedReference<ReadLock> copy = readLockRef.copy();
                                        if (flamdex instanceof RawFlamdexReader) {
                                            return new RawCachedFlamdexReader(
                                                                              new MemoryReservationContext(
                                                                                                           memory),
                                                                              (RawFlamdexReader) flamdex,
                                                                              copy, dataset,
                                                                              shardDir.getName(),
                                                                              freeCache);
                                        } else {
                                            return new CachedFlamdexReader(
                                                                           new MemoryReservationContext(
                                                                                                        memory),
                                                                           flamdex, copy, dataset,
                                                                           shardDir.getName(),
                                                                           freeCache);
                                        }
                                    }
                                };
                        newShard =
                                new Shard(ReloadableSharedReference.create(loader), readLockRef,
                                          shardVersion, canonicalShardDir, dataset, shardId);
                    } catch (Throwable t) {
                        Closeables2.closeQuietly(readLockRef, log);
                        throw Throwables2.propagate(t, IOException.class);
                    }

                    try {
                        final AtomicSharedReference<Shard> shard;
                        if (newDatasetShards.containsKey(shardId)) {
                            shard = newDatasetShards.get(shardId);
                            final SharedReference<Shard> current = shard.getCopy();
                            try {
                                if (current == null
                                        || shardVersion > current.get().getShardVersion()) {
                                    log.debug("loading shard " + shardId + " from "
                                            + canonicalShardDir);
                                    shard.set(newShard);
                                } else {
                                    Closeables2.closeQuietly(newShard, log);
                                }
                            } finally {
                                Closeables2.closeQuietly(current, log);
                            }
                        } else if (oldDatasetShards.containsKey(shardId)) {
                            shard = oldDatasetShards.get(shardId);
                            final SharedReference<Shard> oldShard = shard.getCopy();
                            try {
                                if (shouldReloadShard(oldShard, canonicalShardDir, shardVersion)) {
                                    log.debug("loading shard " + shardId + " from "
                                            + canonicalShardDir);
                                    shard.set(newShard);
                                } else {
                                    Closeables2.closeQuietly(newShard, log);
                                }
                            } finally {
                                Closeables2.closeQuietly(oldShard, log);
                            }
                        } else {
                            shard = AtomicSharedReference.create(newShard);
                            log.debug("loading shard " + shardId + " from " + canonicalShardDir);
                        }
                        if (shard != null) {
                            newDatasetShards.put(shardId, shard);
                        }
                    } catch (Throwable t) {
                        Closeables2.closeQuietly(newShard, log);
                        throw Throwables2.propagate(t, IOException.class);
                    }

                } catch (IOException e) {
                    log.error("error loading shard at " + shardDir.getAbsolutePath(), e);
                }
            }

            if (newDatasetShards.size() > 0) {
                newShards.put(dataset, newDatasetShards);
            }

            for (final String shardId : oldDatasetShards.keySet()) {
                if (!newDatasetShards.containsKey(shardId)) {
                    try {
                        oldDatasetShards.get(shardId).unset();
                    } catch (IOException e) {
                        log.error("error closing shard " + shardId, e);
                    }
                }
            }
        }

        this.shards = newShards;

        final List<ShardInfo> shardList = buildShardList();
        final List<ShardInfo> oldShardList = this.shardList;
        if (oldShardList == null || !oldShardList.equals(shardList)) {
            this.shardList = shardList;
        }

        final List<DatasetInfo> datasetList = buildDatasetList();
        final List<DatasetInfo> oldDatasetList = this.datasetList;
        if (oldDatasetList == null || !oldDatasetList.equals(datasetList)) {
            this.datasetList = datasetList;
        }
    }

    private static boolean shouldReloadShard(SharedReference<Shard> ref,
                                             String canonicalShardDir,
                                             long shardVersion) {
        if (ref == null) {
            return true;
        }
        final Shard oldReader = ref.get();
        return shardVersion > oldReader.getShardVersion()
                || (shardVersion == oldReader.getShardVersion() && (!canonicalShardDir.equals(oldReader.getIndexDir())));
    }

    private List<ShardInfo> buildShardList() throws IOException {
        final Map<String, Map<String, AtomicSharedReference<Shard>>> localShards = shards;
        final List<ShardInfo> ret = new ArrayList<ShardInfo>();
        for (final Map<String, AtomicSharedReference<Shard>> map : localShards.values()) {
            for (final String shardName : map.keySet()) {
                final SharedReference<Shard> ref = map.get(shardName).getCopy();
                try {
                    if (ref != null) {
                        final Shard shard = ref.get();
                        ret.add(new ShardInfo(shard.getDataset(), shardName,
                                              shard.getLoadedMetrics(), shard.getNumDocs(),
                                              shard.getShardVersion()));
                    }
                } finally {
                    Closeables2.closeQuietly(ref, log);
                }
            }
        }
        Collections.sort(ret, new Comparator<ShardInfo>() {
            @Override
            public int compare(ShardInfo o1, ShardInfo o2) {
                final int c = o1.dataset.compareTo(o2.dataset);
                if (c != 0) {
                    return c;
                }
                return o1.shardId.compareTo(o2.shardId);
            }
        });
        return ret;
    }

    private List<DatasetInfo> buildDatasetList() throws IOException {
        final Map<String, Map<String, AtomicSharedReference<Shard>>> localShards = shards;
        final List<DatasetInfo> ret = Lists.newArrayList();
        for (final Map.Entry<String, Map<String, AtomicSharedReference<Shard>>> e : localShards.entrySet()) {
            final String dataset = e.getKey();
            final Map<String, AtomicSharedReference<Shard>> map = e.getValue();
            final List<ShardInfo> shardList = Lists.newArrayList();
            final Set<String> intFields = Sets.newHashSet();
            final Set<String> stringFields = Sets.newHashSet();
            final Set<String> metrics = Sets.newHashSet();
            for (final String shardName : map.keySet()) {
                final SharedReference<Shard> ref = map.get(shardName).getCopy();
                try {
                    if (ref != null) {
                        final Shard shard = ref.get();
                        shardList.add(new ShardInfo(shard.getDataset(), shardName,
                                                    shard.getLoadedMetrics(), shard.getNumDocs(),
                                                    shard.getShardVersion()));
                        intFields.addAll(shard.getIntFields());
                        stringFields.addAll(shard.getStringFields());
                        metrics.addAll(shard.getAvailableMetrics());
                    }
                } finally {
                    Closeables2.closeQuietly(ref, log);
                }
            }
            ret.add(new DatasetInfo(dataset, shardList, intFields, stringFields, metrics));
        }
        return ret;
    }

    @Override
    public List<ShardInfo> handleGetShardList() {
        return shardList;
    }

    @Override
    public List<DatasetInfo> handleGetDatasetList() {
        return datasetList;
    }

    @Override
    public ImhotepStatusDump handleGetStatusDump() {
        final Map<String, Map<String, AtomicSharedReference<Shard>>> localShards = shards;

        final long usedMemory = memory.usedMemory();
        final long totalMemory = memory.totalMemory();

        final List<ImhotepStatusDump.SessionDump> openSessions =
                getSessionManager().getSessionDump();

        final List<ImhotepStatusDump.ShardDump> shards =
                new ArrayList<ImhotepStatusDump.ShardDump>();
        for (final String dataset : localShards.keySet()) {
            for (final String shardId : localShards.get(dataset).keySet()) {
                final SharedReference<Shard> ref = localShards.get(dataset).get(shardId).getCopy();
                try {
                    if (ref != null) {
                        final Shard shard = ref.get();
                        try {
                            shards.add(new ImhotepStatusDump.ShardDump(shardId, dataset,
                                                                       shard.getNumDocs(),
                                                                       shard.getMetricDump()));
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                } finally {
                    Closeables2.closeQuietly(ref, log);
                }
            }
        }

        return new ImhotepStatusDump(usedMemory, totalMemory, openSessions, shards);
    }

    @Override
    public List<String> getShardIdsForSession(String sessionId) {
        return getSessionManager().getShardIdsForSession(sessionId);
    }

    @Override
    public String handleOpenSession(final String dataset,
                                    final List<String> shardRequestList,
                                    final String username,
                                    final String ipAddress,
                                    final int clientVersion,
                                    final int mergeThreadLimit,
                                    final boolean optimizeGroupZeroLookups,
                                    String sessionId,
                                    AtomicLong tempFileSizeBytesLeft,
                                    final boolean useNativeFtgs) throws ImhotepOutOfMemoryException {
        final Map<String, Map<String, AtomicSharedReference<Shard>>> localShards = this.shards;
        checkDatasetExists(localShards, dataset);

        if (Strings.isNullOrEmpty(sessionId)) {
            sessionId = generateSessionId();
        }

        final Map<String, AtomicSharedReference<Shard>> datasetShards = localShards.get(dataset);
        final Map<String, Pair<ShardId, CachedFlamdexReaderReference>> flamdexReaders = Maps.newHashMap();
        boolean allFlamdexReaders = true;
        for (final String shardName : shardRequestList) {
            if (!datasetShards.containsKey(shardName)) {
                throw new IllegalArgumentException("this service does not have shard " + shardName
                        + " in dataset " + dataset);
            }
            final SharedReference<Shard> ref = datasetShards.get(shardName).getCopy();
            try {
                if (ref == null) {
                    throw new IllegalArgumentException("this service does not have shard "
                            + shardName + " in dataset " + dataset);
                }
                final CachedFlamdexReaderReference cachedFlamdexReaderReference;
                final ShardId shardId;
                try {
                    final Shard shard = ref.get();
                    shardId = shard.getShardId();
                    final SharedReference<CachedFlamdexReader> reference = shard.getRef();
                    if (reference.get() instanceof RawCachedFlamdexReader) {
                        cachedFlamdexReaderReference =
                            new RawCachedFlamdexReaderReference((SharedReference<RawCachedFlamdexReader>) (SharedReference) reference);
                    } else {
                        allFlamdexReaders = false;
                        cachedFlamdexReaderReference = new CachedFlamdexReaderReference(reference);
                    }
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                flamdexReaders.put(shardName, Pair.of(shardId, cachedFlamdexReaderReference));
            } finally {
                Closeables2.closeQuietly(ref, log);
            }
        }

        final Map<ShardId, CachedFlamdexReaderReference> flamdexes = Maps.newHashMap();
        final ImhotepLocalSession[] localSessions;
        localSessions = new ImhotepLocalSession[shardRequestList.size()];
        try {
            for (int i = 0; i < shardRequestList.size(); ++i) {
                final String shardId = shardRequestList.get(i);
                final Pair<ShardId, CachedFlamdexReaderReference> pair =
                        flamdexReaders.get(shardId);
                final CachedFlamdexReaderReference cachedFlamdexReaderReference = pair.getSecond();
                final InstrumentedFlamdexReader instrumentedFlamdexReader =
                    new InstrumentedFlamdexReader(cachedFlamdexReaderReference,
                                                  dataset, sessionId, username);
                final Instrumentation.Observer observer =
                    new Instrumentation.Observer() {
                        public void onEvent(Instrumentation.Event event) {
                            //                            log.info("*** inst event: " + event);
                            System.err.println("*** inst event: " + event);
                        } };
                instrumentedFlamdexReader.addObserver(InstrumentedFlamdexReader.OPEN_EVENT, observer);
                instrumentedFlamdexReader.addObserver(InstrumentedFlamdexReader.CLOSE_EVENT, observer);
                try {
                    flamdexes.put(pair.getFirst(), cachedFlamdexReaderReference);
                    localSessions[i] = useNativeFtgs && allFlamdexReaders ?
                        new ImhotepNativeLocalSession(instrumentedFlamdexReader,
                                                      new MemoryReservationContext(memory),
                                                      tempFileSizeBytesLeft) :
                        new ImhotepJavaLocalSession(instrumentedFlamdexReader,
                                                    this.shardTempDirectory,
                                                    new MemoryReservationContext(memory),
                                                    tempFileSizeBytesLeft);
                } catch (RuntimeException e) {
                    Closeables2.closeQuietly(instrumentedFlamdexReader, log);
                    localSessions[i] = null;
                    throw e;
                } catch (ImhotepOutOfMemoryException e) {
                    Closeables2.closeQuietly(instrumentedFlamdexReader, log);
                    localSessions[i] = null;
                    throw e;
                }
            }
            final ImhotepSession session =
                new MTImhotepLocalMultiSession(localSessions,
                                               new MemoryReservationContext(memory),
                                               executor,
                                               tempFileSizeBytesLeft,
                                               useNativeFtgs && allFlamdexReaders);
            getSessionManager().addSession(sessionId, session, flamdexes, username,
                                           ipAddress, clientVersion, dataset);
        } catch (RuntimeException e) {
            closeNonNullSessions(localSessions);
            throw e;
        } catch (ImhotepOutOfMemoryException e) {
            closeNonNullSessions(localSessions);
            throw e;
        }

        return sessionId;
    }

    private static void checkDatasetExists(Map<String, Map<String, AtomicSharedReference<Shard>>> shards,
                                           String dataset) {
        if (!shards.containsKey(dataset)) {
            throw new IllegalArgumentException("this service does not have dataset " + dataset);
        }
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
        executor.shutdownNow();
        shardReload.shutdown();
        heartBeat.shutdown();
    }

    @Export(name = "loaded-shard-count", doc = "number of loaded shards for each dataset", expand = true)
    public Map<String, Integer> getLoadedShardCount() {
        final Map<String, Map<String, AtomicSharedReference<Shard>>> shards = this.shards;
        final Map<String, Integer> ret = Maps.newTreeMap();
        for (final String dataset : shards.keySet()) {
            ret.put(dataset, shards.get(dataset).size());
        }
        return ret;
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
