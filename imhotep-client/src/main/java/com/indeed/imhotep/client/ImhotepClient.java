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
 package com.indeed.imhotep.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.BatchRemoteImhotepMultiSession;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.DynamicIndexSubshardDirnameUtil;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.commands.OpenSessionData;
import com.indeed.imhotep.commands.OpenSessions;
import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.imhotep.shardmasterrpc.RequestResponseClient;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import io.opentracing.ActiveSpan;
import io.opentracing.NoopActiveSpanSource;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author jsgroth
 */
public class ImhotepClient
    implements Closeable, Instrumentation.Provider {
    private static final Logger log = Logger.getLogger(ImhotepClient.class);
    private final HostsReloader hostsSource;
    private final ExecutorService rpcExecutor;
    private final ScheduledExecutorService reloader;
    private final ImhotepClientMetadataReloader datasetMetadataReloader;
    private final Supplier<ShardMaster> shardMasterSupplier;
    private List<Host> imhotepDaemonsOverride;

    private final Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    /**
     * create an imhotep client that will periodically reload its list of hosts from a text file
     * @param hostsFile hosts file for shardmasters
     */
    public ImhotepClient(final String hostsFile) {
        this(new FileHostsReloader(hostsFile));
    }

    /**
     * create an imhotep client with a static list of hosts
     * @param hosts list of shardmaster hosts
     */
    public ImhotepClient(final List<Host> hosts) {
         this(new DummyHostsReloader(hosts));
    }

    public ImhotepClient(final String zkNodes, final boolean readHostsBeforeReturning) {
        this(new ZkHostsReloader(zkNodes, readHostsBeforeReturning));
    }

    public ImhotepClient(final String zkNodes, final String zkPath, final boolean readHostsBeforeReturning) {
        this(new ZkHostsReloader(zkNodes, zkPath, readHostsBeforeReturning));
    }

    public ImhotepClient(final HostsReloader hostsSource) {

        this.hostsSource = hostsSource;

        this.shardMasterSupplier = getShardMasterSupplier();

        rpcExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(@Nonnull final Runnable r) {
                final Thread t = new Thread(r, "ImhotepClient.RPCThread");
                t.setDaemon(true);
                return t;
            }
        });

        reloader = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(@Nonnull final Runnable r) {
                final Thread t = new Thread(r, "ImhotepClient.Reloader");
                t.setDaemon(true);
                return t;
            }
        });
        reloader.scheduleAtFixedRate(hostsSource, 60L, 60L, TimeUnit.SECONDS);

        datasetMetadataReloader = new ImhotepClientMetadataReloader(shardMasterSupplier);
        datasetMetadataReloader.run();
        reloader.scheduleAtFixedRate(datasetMetadataReloader, 60L, 60L, TimeUnit.SECONDS);
    }

    /**
     * Returns a sorted list of dataset names.
     */
    public List<String> getDatasetNames() {
        final List<String> result = new ArrayList<>(getDatasetToDatasetInfo().keySet());
        Collections.sort(result);
        return result;
    }

    /**
     * Returns a map of dataset names to their field lists. Doesn't include shard lists.
     */
    public Map<String, DatasetInfo> getDatasetToDatasetInfo() {
        return datasetMetadataReloader.getDatasetToDatasetInfo();
    }

    /**
     * Returns an object containing field lists for the requested dataset. Doesn't include shard list.
     */
    public DatasetInfo getDatasetInfo(String datasetName) {
        final DatasetInfo datasetInfo = getDatasetToDatasetInfo().get(datasetName);
        if(datasetInfo == null) {
            throw new IllegalArgumentException("Dataset not found: " + datasetName);
        }
        return datasetInfo;
    }

    private static List<Shard> keepLatestVersionsOnly(List<Shard> shardList) {
        final Map<String,Shard> latestVersionMap = new HashMap<>();
        for(Shard shard: shardList) {
            if(!latestVersionMap.containsKey(shard.shardId) || latestVersionMap.get(shard.shardId).version < shard.version) {
                latestVersionMap.put(shard.shardId, shard);
            }
        }

        final List<Shard> ret = Lists.newArrayList(latestVersionMap.values());
        Collections.sort(ret);
        return ret;
    }

    /**
     * Returns a list of non-overlapping Imhotep shards for the specified dataset and time range.
     * Shards in the list are sorted chronologically. If imhotepDaemonsOverride is non-null, then
     * this will assign the returned shards to those daemons using a hash of the dataset, shardId,
     * and version.
     */
    public List<Shard> findShardsForTimeRange(final String dataset, final DateTime start, final DateTime end) {
        if(start == null || end == null) {
            throw new IllegalArgumentException("start and end times can't be null");
        }
        if(!end.isAfter(start)) {
            throw new IllegalArgumentException("Illegal time range requested: " + start.toString() + " to " + end.toString());
        }
        // get shards intersecting with (start,end) time range
        final List<Shard> allShards = getAllShardsForTimeRange(dataset, start, end);
        final List<Shard> shardsForTime = keepLatestVersionsOnly(allShards);
        return removeIntersectingShards(shardsForTime, dataset, start);
    }

    /** Returns all the shards that exist for the specified dataset and overlap with the provided time range */
    @VisibleForTesting
    protected List<Shard> getAllShardsForTimeRange(String dataset, DateTime start, DateTime end) {
        final long startUnixtime = start.getMillis();
        final long endUnixtime = end.getMillis();
        try {
            final List<Shard> shardsInTime = shardMasterSupplier.get().getShardsInTime(dataset, startUnixtime, endUnixtime);
            if(imhotepDaemonsOverride == null) {
                return shardsInTime;
            }
            return shardsInTime.stream().map(shard -> shard.withHost(computeOverrideHost(dataset, shard))).collect(Collectors.toList());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static final ThreadLocal<HashFunction> HASH_FUNCTION = new ThreadLocal<HashFunction>() {
        @Override
        protected HashFunction initialValue() {
            return Hashing.murmur3_32((int)1515546902721L);
        }
    };

    private Host computeOverrideHost(final String dataset, final Shard shard) {
        final long hash = Math.abs((long) HASH_FUNCTION.get().newHasher().putInt(dataset.hashCode()).putInt(shard.shardId.hashCode()).putLong(shard.version).hash().asInt());
        return imhotepDaemonsOverride.get((int) (hash % imhotepDaemonsOverride.size()));
    }

    // we are truncating the shard start point as part of removeIntersectingShards so we make a wrapper for the LocatedShardInfo
    private static class ShardTruncatedStart {
        private final Shard shard;
        private final DateTime start;
        private final DateTime end;
        private final long version;

        private ShardTruncatedStart(final Shard shard, final DateTime start) {
            this.shard = shard;
            this.start = start;
            this.end = shard.getEnd();
            this.version = shard.getVersion();
        }
    }

    /**
     * Returns a non-intersecting list of shard ids and versions chosen from the shardsForTime list.
     * Shards in the list are sorted chronologically.
     */
    @VisibleForTesting
    static List<Shard> removeIntersectingShards(final List<Shard> shardsForTime, final String dataset, final DateTime start) {
        // we have to limit shard start times to the requested start time to avoid
        // longer shards with the earlier start time taking precedence over newer smaller shards
        final Map<Integer, List<ShardTruncatedStart>> shardsForTimeTruncatedPerSubshard = new HashMap<>();
        for (final Shard shard : shardsForTime) {
            ShardInfo.DateTimeRange range = shard.getRange();
            if (range == null) {
                log.warn("Unparseable shard id encountered in dataset '" + dataset + "': " + shard.getShardId());
                continue;
            }
            DateTime shardStart = range.start;
            if (start.isAfter(range.start)) {
                shardStart = start;
            }

            // For the case of sub-sharded indexes (i.e. dynamic indexes)
            final int subshardId;
            if (DynamicIndexSubshardDirnameUtil.isValidDynamicShardId(shard.getShardId())) {
                subshardId = DynamicIndexSubshardDirnameUtil.parseSubshardIdFromShardId(shard.getShardId());
            } else {
                subshardId = 0;
            }
            shardsForTimeTruncatedPerSubshard
                    .computeIfAbsent(subshardId, (ignored) -> new ArrayList<>())
                    .add(new ShardTruncatedStart(shard, shardStart));
        }

        final List<Shard> chosenShards = Lists.newArrayList();
        for (final List<ShardTruncatedStart> shardsForTimeTruncated : shardsForTimeTruncatedPerSubshard.values()) {
            // now we need to resolve potential time overlaps in shards
            // sort by: start date asc, version desc
            Collections.sort(shardsForTimeTruncated, new Comparator<ShardTruncatedStart>() {
                @Override
                public int compare(final ShardTruncatedStart o1, final ShardTruncatedStart o2) {
                    final int c = o1.start.compareTo(o2.start);
                    if (c != 0) {
                        return c;
                    }
                    return -Longs.compare(o1.version, o2.version);
                }
            });

            DateTime processedUpTo = new DateTime(-2000000, 1, 1, 0, 0);  // 2M BC

            for (final ShardTruncatedStart shard : shardsForTimeTruncated) {
                if (!shard.start.isBefore(processedUpTo)) {
                    chosenShards.add(shard.shard);
                    processedUpTo = shard.end;
                }
            }
        }
        return chosenShards;
    }

    /**
     * Returns a builder that can be used to initialize an {@link ImhotepSession} instance.
     * @param dataset dataset/index name for the session
     */
    public SessionBuilder sessionBuilder(final String dataset, final DateTime start, final DateTime end) {
        return new SessionBuilder(dataset, start, end);
    }

    /**
     * Constructs {@link ImhotepSession} instances.
     * Set optional parameters and call {@link #build}() to get an instance.
     */
    public class SessionBuilder {
        private final String dataset;
        private final DateTime start;
        private final DateTime end;

        private int mergeThreadLimit = ImhotepRemoteSession.DEFAULT_MERGE_THREAD_LIMIT;
        private String username = "";
        private String clientName = "";
        private byte priority = ImhotepRemoteSession.DEFAULT_PRIORITY;
        private boolean optimizeGroupZeroLookups = false;
        private int socketTimeout = -1;
        private long localTempFileSizeLimit = -1;
        private long daemonTempFileSizeLimit = -1;
        private long sessionTimeout = -1;

        private List<Shard> chosenShards = null;
        private List<Shard> shardsOverride = null;

        private boolean allowSessionForwarding = false;
        private boolean peerToPeerCache = false;
        private boolean useFtgsPooledConnection = false;

        private boolean useBatch = false;

        private int hostCount = 0; // for logging

        public SessionBuilder(final String dataset, final DateTime start, final DateTime end) {
            this.dataset = dataset;
            this.start = start;
            this.end = end;
        }

        public SessionBuilder mergeThreadLimit(final int newMergeThreadLimit) {
            this.mergeThreadLimit = newMergeThreadLimit;
            return this;
        }

        public SessionBuilder socketTimeout(final int newSocketTimeout) {
            socketTimeout = newSocketTimeout;
            return this;
        }
        public SessionBuilder username(final String newUsername) {
            username = newUsername;
            return this;
        }
        public SessionBuilder clientName(final String newClientName) {
            clientName = newClientName;
            return this;
        }

        /**
         * Sets the priority of the session. The higher the number, the higher the priority.
         */
        public SessionBuilder priority(final byte priority) {
            this.priority = priority;
            return this;
        }

        public SessionBuilder optimizeGroupZeroLookups(final boolean newOptimizeGroupZeroLookups) {
            optimizeGroupZeroLookups = newOptimizeGroupZeroLookups;
            return this;
        }

        public SessionBuilder localTempFileSizeLimit(final long newLocalTempFileSizeLimit) {
            localTempFileSizeLimit = newLocalTempFileSizeLimit;
            return this;
        }

        public SessionBuilder daemonTempFileSizeLimit(final long newDaemonTempFileSizeLimit) {
            daemonTempFileSizeLimit = newDaemonTempFileSizeLimit;
            return this;
        }

        public SessionBuilder shardsOverride(final Collection<Shard> requiredShards) {
            this.shardsOverride = Lists.newArrayList(requiredShards);
            return this;
        }

        public SessionBuilder allowPeerToPeerCache(final boolean peerToPeerCache) {
            this.peerToPeerCache = peerToPeerCache;
            return this;
        }

        public SessionBuilder useFtgsPooledConnection(final boolean useFtgsPooledConnection) {
            this.useFtgsPooledConnection = useFtgsPooledConnection;
            return this;
        }

        /**
         * IMTEPD-314: Indicates whether the request is allowed to be forwarded to another instance of Imhotep running
         *             on a different port. Used for development simplicity of alternate implementations.
         */
        public SessionBuilder allowSessionForwarding(boolean allow) {
            this.allowSessionForwarding = allow;
            return this;
        }

        /*
            @return session timeout in milliseconds
         */
        @Deprecated
        public long getSessionTimeout() {
            return sessionTimeout;
        }

        /*  @deprecated use sessionTimeout()
            @param sessionTimeout session timeout in milliseconds, the default is 30 minutes
         */
        @Deprecated
        public void setSessionTimeout(final long sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
        }

        /*
            @param sessionTimeout session timeout in milliseconds, the default is 30 minutes
         */
        public SessionBuilder sessionTimeout(final long sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
            return this;
        }

        public SessionBuilder useBatch(final boolean useBatch) {
            this.useBatch = useBatch;
            return this;
        }

        /**
         * Returns shards that were selected for the time range requested in the constructor.
         * Shards in the list are sorted chronologically.
         */
        public List<Shard> getChosenShards() {
            if(chosenShards == null) {
                this.chosenShards = findShardsForTimeRange(dataset, start, end);
            }
            return Lists.newArrayList(chosenShards);
        }

        /**
         * Returns a list of time intervals within the requested [start, end) range that are not covered by available shards.
         * Intervals in the list are sorted chronologically.
         */
        public List<Interval> getTimeIntervalsMissingShards() {
            // expects the returned shards to be sorted by start time
            final List<Shard> chosenShards = getChosenShards();

            final List<Interval> timeIntervalsMissingShards = Lists.newArrayList();
            DateTime processedUpTo = start;
            for(final Shard shard : chosenShards) {
                if(processedUpTo.isBefore(shard.getStart())) {
                    timeIntervalsMissingShards.add(new Interval(processedUpTo, shard.getStart()));
                }
                processedUpTo = shard.getEnd();
            }

            if(processedUpTo.isBefore(end)) {
                timeIntervalsMissingShards.add(new Interval(processedUpTo, end));
            }
            return timeIntervalsMissingShards;
        }

        // call this method after build()
        public int getHostCount() {
            return hostCount;
        }

        /**
         * Constructs an {@link ImhotepSession} instance.
         */
        public ImhotepSession build() {
            if(username == null) {
                username = ImhotepRemoteSession.getUsername();
            }
            final List<Shard> locatedShards = shardsOverride != null ? shardsOverride : getChosenShards();
            if(locatedShards == null || locatedShards.isEmpty()) {
                throw new IllegalArgumentException("No shards: no data available for the requested dataset and time range");
            }
            final Map<Host, List<Shard>> hostsToShardsMap = orderShardsByHost(locatedShards);
            hostCount = hostsToShardsMap.size();
            if (useBatch) {
                return new BatchRemoteImhotepMultiSession(
                        new OpenSessions(
                                hostsToShardsMap,
                                new OpenSessionData(
                                        dataset,
                                        mergeThreadLimit,
                                        username,
                                        clientName,
                                        priority,
                                        optimizeGroupZeroLookups,
                                        daemonTempFileSizeLimit,
                                        sessionTimeout,
                                        useFtgsPooledConnection
                                ),
                                socketTimeout,
                                localTempFileSizeLimit,
                                allowSessionForwarding,
                                peerToPeerCache
                        )
                );
            } else {
                return getSessionForShards(
                        dataset, hostsToShardsMap, mergeThreadLimit, username, clientName, priority, optimizeGroupZeroLookups,
                        socketTimeout, localTempFileSizeLimit, daemonTempFileSizeLimit, sessionTimeout,
                        allowSessionForwarding, peerToPeerCache, useFtgsPooledConnection
                );
            }
        }
    }

    private ImhotepSession getSessionForShards(final String dataset, final Map<Host, List<Shard>> hostToShardsMap,
                                               final int mergeThreadLimit, final String username, final String clientName, final byte priority,
                                               final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                               final long localTempFileSizeLimit, final long daemonTempFileSizeLimit,
                                               final long sessionTimeout,
                                               final boolean allowSessionForwarding,
                                               final boolean p2pCache,
                                               final boolean useFtgsPooledConnection) {

        final AtomicLong localTempFileSizeBytesLeft = localTempFileSizeLimit > 0 ? new AtomicLong(localTempFileSizeLimit) : null;
        try {
            final String sessionId = UUID.randomUUID().toString();
            ImhotepRemoteSession[] remoteSessions = internalGetSession(dataset, hostToShardsMap, mergeThreadLimit, username,
                    clientName, priority, optimizeGroupZeroLookups, socketTimeout, sessionId, daemonTempFileSizeLimit,
                    localTempFileSizeBytesLeft, sessionTimeout, allowSessionForwarding, p2pCache, useFtgsPooledConnection);

            final InetSocketAddress[] nodes = new InetSocketAddress[remoteSessions.length];
            for (int i = 0; i < remoteSessions.length; i++) {
                nodes[i] = remoteSessions[i].getInetSocketAddress();
            }
            return new RemoteImhotepMultiSession(remoteSessions, sessionId, nodes, localTempFileSizeLimit, localTempFileSizeBytesLeft, username, clientName, priority);
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, ImhotepKnownException.class);
            throw new RuntimeException("unable to open session",  e);
        }
    }

    @Nonnull
    private ImhotepRemoteSession[]
        internalGetSession(final String dataset,
                           final Map<Host, List<Shard>> shardRequestMap,
                           final int mergeThreadLimit,
                           final String username,
                           final String clientName,
                           final byte priority,
                           final boolean optimizeGroupZeroLookups,
                           final int socketTimeout,
                           @Nullable final String sessionId,
                           final long tempFileSizeLimit,
                           @Nullable final AtomicLong tempFileSizeBytesLeft,
                           final long sessionTimeout,
                           boolean allowSessionForwarding,
                           final boolean p2pCache,
                           final boolean useFtgsPooledConnection) {

        final ExecutorService executor = Executors.newCachedThreadPool();
        final List<Future<ImhotepRemoteSession>> futures = new ArrayList<>(shardRequestMap.size());
        final Tracer tracer = GlobalTracer.get();
        try {
            for (final Map.Entry<Host, List<Shard>> entry : shardRequestMap.entrySet()) {
                final Host host = entry.getKey();
                final List<Shard> shardList = entry.getValue();

                final long numDocs = shardRequestMap.get(host).stream().mapToInt(Shard::getNumDocs).asLongStream().sum();
                final ActiveSpan.Continuation continuation = getContinuation(tracer);

                futures.add(executor.submit(new Callable<ImhotepRemoteSession>() {
                    @Override
                    public ImhotepRemoteSession call() throws IOException, ImhotepOutOfMemoryException {
                        try (final ActiveSpan parentSpan = continuation.activate()) {
                            return ImhotepRemoteSession.openSession(host.hostname, host.port,
                                    dataset, shardList,
                                    mergeThreadLimit,
                                    username,
                                    clientName,
                                    priority,
                                    optimizeGroupZeroLookups,
                                    socketTimeout,
                                    sessionId,
                                    tempFileSizeLimit,
                                    tempFileSizeBytesLeft,
                                    sessionTimeout,
                                    allowSessionForwarding,
                                    numDocs,
                                    p2pCache,
                                    useFtgsPooledConnection);
                        }
                    }
                }));
            }
        } finally {
            executor.shutdown();
        }

        final ImhotepRemoteSession[] remoteSessions =
            new ImhotepRemoteSession[shardRequestMap.size()];
        Throwable error = null;
        for (int i = 0; i < futures.size(); ++i) {
            try {
                remoteSessions[i] = futures.get(i).get();
                remoteSessions[i].addObserver(new Instrumentation.Observer() {
                        // Just relay to our observers.
                        public void onEvent(final Instrumentation.Event event) {
                            instrumentation.fire(event);
                        }
                    });
            } catch (final ExecutionException e) {
                error = e.getCause();
                if (!(error instanceof ImhotepKnownException)) {
                    log.error("exception while opening session", e);
                }
            } catch (final InterruptedException e) {
                log.error("interrupted while opening session", e);
                error = e;
            }
        }

        if (error != null) {
            for (final ImhotepRemoteSession session : remoteSessions) {
                if (session != null) {
                    try {
                        session.close();
                    } catch (final RuntimeException e) {
                        log.error("exception while closing session", e);
                    }
                }
            }
            throw Throwables.propagate(error);
        }
        return remoteSessions;
    }

    private ActiveSpan.Continuation getContinuation(final Tracer tracer) {
        final ActiveSpan activeSpan = tracer.activeSpan();
        if (activeSpan != null) {
            return activeSpan.capture();
        } else {
            return NoopActiveSpanSource.NoopContinuation.INSTANCE;
        }
    }

    private Map<Host, List<Shard>> orderShardsByHost(final Collection<Shard> requestedShards) {
        final Map<Host, List<Shard>> shardRequestMap = new TreeMap<>();
        for (final Shard shard : requestedShards) {
            final Host host = shard.getServer();
            if (host == null) {
                throw new RuntimeException("something has gone horribly wrong");
            }

            if (!shardRequestMap.containsKey(host)) {
                shardRequestMap.put(host, new ArrayList<>());
            }

            shardRequestMap.get(host).add(shard);
        }
        return shardRequestMap;
    }

    /**
     * Queries a completed shard list from the servers and returns it grouped by dataset.
     * Note that with a millions of shards this request is slow and can use gigabytes of RAM.
     */
    public Map<String, Collection<ShardInfo>> queryDatasetToFullShardList() {
        try {
            return shardMasterSupplier.get().getShardList();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close() throws IOException {
        rpcExecutor.shutdownNow();
        reloader.shutdown();
        hostsSource.shutdown();

        try {
            if (!rpcExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IOException("RPC executor failed to terminate in time");
            }
            if (!reloader.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IOException("reloader failed to terminate in time");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean isConnectionHealthy() {
        return hostsSource.isLoadedDataSuccessfullyRecently() && datasetMetadataReloader.isLoadedDataSuccessfullyRecently();
    }

    public void addObserver(final Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    public void removeObserver(final Instrumentation.Observer observer) {
        instrumentation.removeObserver(observer);
    }

    /**
     * Returns a list of hostnames and ports where all the ShardMaster servers in use by this client can be reached.
     */
    public List<Host> getServerHosts() {
        return new ArrayList<>(hostsSource.getHosts());
    }

    /**
     * This is a dangerous operation used in special circumstances when field list in the ShardMaster DB
     * becomes desynchronized from the state of the shards in the file system.
     */
    private void resetFieldsForDataset(String dataset) {
        try {
            shardMasterSupplier.get().refreshFieldsForDataset(dataset);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }


    private Supplier<ShardMaster> getShardMasterSupplier() {
        return () -> {
            final List<Host> hosts = hostsSource.getHosts();
            if(hosts.isEmpty()) {
                throw new RuntimeException("There are no shardmasters");
            }
            return new RequestResponseClient(hosts);
        };
    }

    public void setImhotepDaemonsOverride(String hostsString) {
        if(Strings.isNullOrEmpty(hostsString)) {
            return;
        }
        this.imhotepDaemonsOverride = parseHostsList(hostsString);
        imhotepDaemonsOverride.sort(Comparator.naturalOrder());
    }


    private List<Host> parseHostsList(String value) {
        final List<Host> hosts = new ArrayList<>();
        for (final String hostString : value.split(",")) {
            try {
                final Host host = Host.valueOf(hostString.trim());
                hosts.add(host);
            } catch (final IllegalArgumentException e) {
                log.warn("Failed to parse host " + hostString + ". Ignore it.", e);
            }
        }
        return hosts;
    }
}
