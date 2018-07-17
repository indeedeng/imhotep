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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.*;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.shardmasterrpc.RequestResponseClient;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
     * Shards in the list are sorted chronologically.
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
            return shardMasterSupplier.get().getShardsInTime(dataset, startUnixtime, endUnixtime);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
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

        private boolean allowSessionForwarding = false;

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
            return getSessionForShards(
                    dataset, hostsToShardsMap, mergeThreadLimit, username, clientName, optimizeGroupZeroLookups,
                    socketTimeout, localTempFileSizeLimit, daemonTempFileSizeLimit, sessionTimeout,
                    allowSessionForwarding
            );
        }
    }

    private ImhotepSession getSessionForShards(final String dataset, final Map<Host, List<Shard>> hostToShardsMap,
                                               final int mergeThreadLimit, final String username, final String clientName,
                                               final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                               final long localTempFileSizeLimit, final long daemonTempFileSizeLimit,
                                               final long sessionTimeout,
                                               final boolean allowSessionForwarding) {

        final AtomicLong localTempFileSizeBytesLeft = localTempFileSizeLimit > 0 ? new AtomicLong(localTempFileSizeLimit) : null;
        try {
            final String sessionId = UUID.randomUUID().toString();
            ImhotepRemoteSession[] remoteSessions = internalGetSession(dataset, hostToShardsMap, mergeThreadLimit, username, clientName, optimizeGroupZeroLookups, socketTimeout, sessionId, daemonTempFileSizeLimit, localTempFileSizeBytesLeft, sessionTimeout, allowSessionForwarding);

            final InetSocketAddress[] nodes = new InetSocketAddress[remoteSessions.length];
            for (int i = 0; i < remoteSessions.length; i++) {
                nodes[i] = remoteSessions[i].getInetSocketAddress();
            }
            return new RemoteImhotepMultiSession(remoteSessions, sessionId, nodes, localTempFileSizeLimit, localTempFileSizeBytesLeft, username, clientName);
        } catch (Exception e) {
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
                           final boolean optimizeGroupZeroLookups,
                           final int socketTimeout,
                           @Nullable final String sessionId,
                           final long tempFileSizeLimit,
                           @Nullable final AtomicLong tempFileSizeBytesLeft,
                           final long sessionTimeout,
                           boolean allowSessionForwarding) {

        final ExecutorService executor = Executors.newCachedThreadPool();
        final List<Future<ImhotepRemoteSession>> futures = new ArrayList<>(shardRequestMap.size());
        try {
            for (final Map.Entry<Host, List<Shard>> entry : shardRequestMap.entrySet()) {
                final Host host = entry.getKey();
                final List<Shard> shardList = entry.getValue();

                final long numDocs = shardRequestMap.get(host).stream().mapToInt(Shard::getNumDocs).sum();

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
            throw Throwables.propagate(error);
        }
        return remoteSessions;
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
    public Map<String, List<ShardInfo>> queryDatasetToFullShardList() {
        try {
            return shardMasterSupplier.get().getShardList();
        } catch (IOException e) {
            log.error("Could not get shard list", e);
            return Collections.emptyMap();
        }
    }

    // TODO: The hosts known by ImhotepClient are now SMs instead of IDs. Maybe forward this to a SM?
    @Deprecated
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
     * Returns a list of hostnames and ports where all the ShardMaster servers in use by this client can be reached.
     */
    public List<Host> getServerHosts() {
        return new ArrayList<>(hostsSource.getHosts());
    }

    // TODO: maybe replace this with a smarter scheme (power of two choices, ect)
    private Supplier<ShardMaster> getShardMasterSupplier() {
        return () -> {
            Host host = hostsSource.getHosts().get((int)(Math.random()*hostsSource.getHosts().size()));
            log.info("Host is: " + host);
            return new RequestResponseClient(host);
        };
    }

}
