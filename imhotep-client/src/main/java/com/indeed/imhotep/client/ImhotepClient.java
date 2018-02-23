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
 package com.indeed.imhotep.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.DynamicIndexSubshardDirnameUtil;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
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

    private final Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    /**
     * create an imhotep client that will periodically reload its list of hosts from a text file
     * @param hostsFile hosts file
     */
    public ImhotepClient(final String hostsFile) {
        this(new FileHostsReloader(hostsFile));
    }

    /**
     * create an imhotep client with a static list of hosts
     * @param hosts list of hosts
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

        datasetMetadataReloader = new ImhotepClientMetadataReloader(hostsSource, rpcExecutor);
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
     * Shards in the list are sorted chronologically.
     */
    public List<Shard> findShardsForTimeRange(final String dataset, final DateTime start, final DateTime end) {
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

        final List<Host> hosts = hostsSource.getHosts();

        final Map<Host, Future<List<ShardInfo>>> futures = Maps.newHashMap();
        for (final Host host : hosts) {
            final Future<List<ShardInfo>> future = rpcExecutor.submit(new Callable<List<ShardInfo>>() {
                @Override
                public List<ShardInfo> call() throws IOException {
                    return ImhotepRemoteSession.getShardlistForTime(host.hostname, host.port, dataset, startUnixtime, endUnixtime);
                }
            });
            futures.put(host, future);
        }

        final Map<ShardInfo, Shard> allShards = Maps.newHashMap();
        for (final Host host : hosts) {
            try {
                final List<ShardInfo> shards = futures.get(host).get();
                for(ShardInfo shard: shards) {
                    final Shard locatedShardInfo = allShards.computeIfAbsent(shard,
                            k -> new Shard(shard.shardId, shard.numDocs, shard.version));
                    locatedShardInfo.getServers().add(host);
                }
            } catch (final ExecutionException | InterruptedException e) {
                throw new RuntimeException("Error getting shard list from " + host, e);
            }
        }
        return Lists.newArrayList(allShards.values());
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
        private boolean optimizeGroupZeroLookups = false;
        private int socketTimeout = -1;
        private long localTempFileSizeLimit = -1;
        private long daemonTempFileSizeLimit = -1;
        private long sessionTimeout = -1;

        private List<Shard> chosenShards = null;
        private List<Shard> shardsOverride = null;
        private boolean useNativeFTGS;

        private boolean allowSessionForwarding = false;

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

        public SessionBuilder useNativeFtgs() {
            useNativeFTGS = true;
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
        public long getSessionTimeout() {
            return sessionTimeout;
        }

        /*
            @param sessionTimeout session timeout in milliseconds, the default is 30 minutes
         */
        public void setSessionTimeout(final long sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
        }

        /**
         * Returns shards that were selected for the time range requested in the constructor.
         * Shards in the list are sorted chronologically.
         */
        public List<Shard> getChosenShards() {
            if(chosenShards == null) {
                if(start == null || end == null) {
                    throw new IllegalArgumentException("start and end times can't be null");
                }
                if(!end.isAfter(start)) {
                    throw new IllegalArgumentException("Illegal time range requested: " + start.toString() + " to " + end.toString());
                }
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
            final ShardsAndDocCounts shardsAndDocCounts = selectHostsForShards(locatedShards);
            return getSessionForShards(
                    dataset, shardsAndDocCounts, mergeThreadLimit, username, clientName, optimizeGroupZeroLookups,
                    socketTimeout, localTempFileSizeLimit, daemonTempFileSizeLimit, useNativeFTGS, sessionTimeout,
                    allowSessionForwarding
            );
        }
    }

    private ImhotepSession getSessionForShards(final String dataset, final ShardsAndDocCounts shardsAndDocCounts,
                                               final int mergeThreadLimit, final String username, final String clientName,
                                               final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                               final long localTempFileSizeLimit, final long daemonTempFileSizeLimit,
                                               final boolean useNativeFtgs, final long sessionTimeout,
                                               final boolean allowSessionForwarding) {

        final AtomicLong localTempFileSizeBytesLeft = localTempFileSizeLimit > 0 ? new AtomicLong(localTempFileSizeLimit) : null;
        try {
            final String sessionId = UUID.randomUUID().toString();
            ImhotepRemoteSession[] remoteSessions = internalGetSession(dataset, shardsAndDocCounts, mergeThreadLimit, username, clientName, optimizeGroupZeroLookups, socketTimeout, sessionId, daemonTempFileSizeLimit, localTempFileSizeBytesLeft, useNativeFtgs, sessionTimeout, allowSessionForwarding);

            final InetSocketAddress[] nodes = new InetSocketAddress[remoteSessions.length];
            for (int i = 0; i < remoteSessions.length; i++) {
                nodes[i] = remoteSessions[i].getInetSocketAddress();
            }
            return new RemoteImhotepMultiSession(remoteSessions, sessionId, nodes, localTempFileSizeLimit, localTempFileSizeBytesLeft);
        } catch (Exception e) {
            throw new RuntimeException("unable to open session",  e);
        }
    }

    @Nonnull
    private ImhotepRemoteSession[]
        internalGetSession(final String dataset,
                           final ShardsAndDocCounts shardsAndDocCounts,
                           final int mergeThreadLimit,
                           final String username,
                           final String clientName,
                           final boolean optimizeGroupZeroLookups,
                           final int socketTimeout,
                           @Nullable final String sessionId,
                           final long tempFileSizeLimit,
                           @Nullable final AtomicLong tempFileSizeBytesLeft,
                           final boolean useNativeFtgs,
                           final long sessionTimeout,
                           boolean allowSessionForwarding) {
        final Map<Host, List<String>> shardRequestMap = shardsAndDocCounts.shardRequestMap;
        final Map<Host, Integer> hostsToDocCounts = shardsAndDocCounts.hostDocCounts;

        final ExecutorService executor = Executors.newCachedThreadPool();
        final List<Future<ImhotepRemoteSession>> futures = new ArrayList<>(shardRequestMap.size());
        try {
            for (final Map.Entry<Host, List<String>> entry : shardRequestMap.entrySet()) {
                final Host host = entry.getKey();
                final List<String> shardList = entry.getValue();
                final long numDocs = hostsToDocCounts.get(host);

                futures.add(executor.submit(new Callable<ImhotepRemoteSession>() {
                    @Override
                    public ImhotepRemoteSession call() throws IOException, ImhotepOutOfMemoryException {
                        return ImhotepRemoteSession.openSession(host.hostname, host.port,
                                                                dataset, shardList,
                                                                mergeThreadLimit,
                                                                username,
                                                                clientName,
                                                                optimizeGroupZeroLookups,
                                                                socketTimeout,
                                                                sessionId,
                                                                tempFileSizeLimit,
                                                                tempFileSizeBytesLeft,
                                                                useNativeFtgs,
                                                                sessionTimeout,
                                                                allowSessionForwarding,
                                                                numDocs);
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
                log.error("exception while opening session", e);
                error = e.getCause();
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
            Throwables.propagate(error);
        }
        return remoteSessions;
    }
    private static class ShardsAndDocCounts {
        final Map<Host, Integer> hostDocCounts;
        final Map<Host, List<String>> shardRequestMap;

        public ShardsAndDocCounts(final Map<Host, List<String>> shardRequestMap, final Map<Host, Integer> hostDocCounts) {
            this.hostDocCounts = hostDocCounts;
            this.shardRequestMap = shardRequestMap;
        }
    }

    private ShardsAndDocCounts selectHostsForShards(final Collection<Shard> requestedShards) {
        final List<Shard> sortedShards = new ArrayList<>(requestedShards);
        sortedShards.sort(new Comparator<Shard>() {
            @Override
            public int compare(final Shard o1, final Shard o2) {
                return -(Integer.compare(o1.numDocs, o2.numDocs));
            }
        });

        final Map<Host, Integer> hostDocCounts = new HashMap<>();
        final Map<Host, List<String>> shardRequestMap = new TreeMap<>();
        for (final Shard shard : sortedShards) {
            final List<Host> potentialHosts = shard.getServers();
            int minHostDocCount = Integer.MAX_VALUE;
            Host minHost = null;
            for (final Host host : potentialHosts) {
                if (!hostDocCounts.containsKey(host)) {
                    hostDocCounts.put(host, 0);
                }
                if (hostDocCounts.get(host) < minHostDocCount) {
                    minHostDocCount = hostDocCounts.get(host);
                    minHost = host;
                }
            }
            if (minHost == null) {
                throw new RuntimeException("something has gone horribly wrong");
            }

            if (!shardRequestMap.containsKey(minHost)) {
                shardRequestMap.put(minHost, new ArrayList<String>());
            }
            shardRequestMap.get(minHost).add(shard.shardId);
            hostDocCounts.put(minHost, hostDocCounts.get(minHost) + shard.numDocs);
        }
        return new ShardsAndDocCounts(shardRequestMap, hostDocCounts);
    }

    /**
     * Queries a completed shard list from the servers and returns it grouped by dataset.
     * Note that with a millions of shards this request is slow and can use gigabytes of RAM.
     */
    public Map<String, List<Shard>> queryDatasetToFullShardList() {
        final Map<Host, List<DatasetInfo>> hostToDatasets= shardListRpc();
        final Map<String, Map<ShardInfo, Shard>> datasetToAllShards = Maps.newHashMap();
        for (Map.Entry<Host, List<DatasetInfo>> entry: hostToDatasets.entrySet()) {
            final Host host = entry.getKey();
            for(DatasetInfo datasetInfo: entry.getValue()) {
                final String datasetName = datasetInfo.getDataset();
                Map<ShardInfo, Shard> shardmapForDataset = datasetToAllShards.computeIfAbsent(datasetName, k -> Maps.newHashMap());
                final Collection<ShardInfo> shards = datasetInfo.getShardList();
                for (ShardInfo shard : shards) {
                    final Shard locatedShardInfo = shardmapForDataset.computeIfAbsent(shard,
                            k -> new Shard(shard.shardId, shard.numDocs, shard.version));
                    locatedShardInfo.getServers().add(host);
                }
            }
        }

        final Map<String, List<Shard>> result = Maps.newHashMap();
        for(Map.Entry<String, Map<ShardInfo, Shard>> entry: datasetToAllShards.entrySet()) {
            result.put(entry.getKey(), Lists.newArrayList(entry.getValue().values()));
        }
        return result;
    }

    private Map<Host, List<DatasetInfo>> shardListRpc() {
        final List<Host> hosts = hostsSource.getHosts();

        final Map<Host, Future<List<DatasetInfo>>> futures = Maps.newHashMap();
        for (final Host host : hosts) {
            final Future<List<DatasetInfo>> future = rpcExecutor.submit(new Callable<List<DatasetInfo>>() {
                @Override
                public List<DatasetInfo> call() throws IOException {
                    return ImhotepRemoteSession.getShardInfoList(host.hostname, host.port);
                }
            });
            futures.put(host, future);
        }

        final Map<Host, List<DatasetInfo>> ret = Maps.newHashMapWithExpectedSize(hosts.size());
        for (final Map.Entry<Host, Future<List<DatasetInfo>>> hostFutureEntry : futures.entrySet()) {
            try {
                final List<DatasetInfo> shards = hostFutureEntry.getValue().get();
                ret.put(hostFutureEntry.getKey(), shards);
            } catch (ExecutionException | InterruptedException e) {
                log.error("error getting shard list from " + hostFutureEntry.getKey(), e);
            }
        }

        return ret;
    }

    public Map<Host, ImhotepStatusDump> getStatusDumps() {
        final List<Host> hosts = hostsSource.getHosts();

        final Map<Host, Future<ImhotepStatusDump>> futures = Maps.newHashMap();
        for (final Host host : hosts) {
            final Future<ImhotepStatusDump> future = rpcExecutor.submit(new Callable<ImhotepStatusDump>() {
                @Override
                public ImhotepStatusDump call() throws IOException {
                    return ImhotepRemoteSession.getStatusDump(host.hostname, host.port);
                }
            });
            futures.put(host, future);
        }

        final Map<Host, ImhotepStatusDump> ret = new HashMap<>();
        for (final Host host : hosts) {
            try {
                final ImhotepStatusDump statusDump = futures.get(host).get();
                ret.put(host, statusDump);
            } catch (final ExecutionException | InterruptedException e) {
                log.error("error getting status dump from " + host, e);
            }
        }
        return ret;
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
     * Returns a list of hostnames and ports where all the Imhotep servers in use by this client can be reached.
     */
    public List<Host> getServerHosts() {
        return new ArrayList<>(hostsSource.getHosts());
    }
}
