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

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.DynamicIndexSubshardDirnameUtil;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.util.core.Pair;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
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
    private final ImhotepClientShardListReloader shardListReloader;

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
        shardListReloader = new ImhotepClientShardListReloader(hostsSource, rpcExecutor);
        shardListReloader.run();
        reloader.scheduleAtFixedRate(shardListReloader, 60L, 60L, TimeUnit.SECONDS);
    }

    public Map<Host, List<DatasetInfo>> getShardList() {
        return shardListReloader.getShardList();
    }

    private static class DatasetNewestShardMetadata {
        long newestShardVersion = Long.MIN_VALUE;
        Collection<String> intFields = Collections.emptyList();
        Collection<String> stringFields = Collections.emptyList();
    }

    // convenience methods
    public Map<String, DatasetInfo> getDatasetToShardList() {
        final Map<Host, List<DatasetInfo>> shardListMap = getShardList();
        return getDatasetToShardList(shardListMap.values());
    }

    public DatasetInfo getDatasetShardInfo(final String dataset) {
        final List<List<DatasetInfo>> restrictedDatasetInfos = new ArrayList<>();
        outer: for (final List<DatasetInfo> hostDatasetInfo : getShardList().values()) {
            for (final DatasetInfo datasetInfo : hostDatasetInfo) {
                if (datasetInfo.getDataset().equals(dataset)) {
                    restrictedDatasetInfos.add(Collections.singletonList(datasetInfo));
                    // Making the assumption that a given dataset only shows up once for a given host.
                    continue outer;
                }
            }
        }
        if (restrictedDatasetInfos.isEmpty()) {
            throw new IllegalArgumentException("Dataset not present in shard lists: " + dataset);
        }
        return getDatasetToShardList(restrictedDatasetInfos).get(dataset);
    }

    static Map<String, DatasetInfo> getDatasetToShardList(final Collection<List<DatasetInfo>> hostsDatasets) {
        final Map<String, DatasetInfo> ret = Maps.newHashMap();
        final Map<String, DatasetNewestShardMetadata> datasetNameToMetadata = Maps.newHashMap();

        for (final List<DatasetInfo> datasetList : hostsDatasets) {
            for (final DatasetInfo dataset : datasetList) {
                ImhotepClient.DatasetNewestShardMetadata datasetNewestShardMetadata = datasetNameToMetadata.get(dataset.getDataset());
                if(datasetNewestShardMetadata == null) {
                    datasetNewestShardMetadata = new ImhotepClient.DatasetNewestShardMetadata();
                    datasetNameToMetadata.put(dataset.getDataset(), datasetNewestShardMetadata);
                }
                DatasetInfo current = ret.get(dataset.getDataset());
                if (current == null) {
                    current = new DatasetInfo(dataset.getDataset(), new HashSet<ShardInfo>(), new HashSet<String>(), new HashSet<String>(), new HashSet<String>());
                    ret.put(dataset.getDataset(), current);
                }
                final Collection<ShardInfo> shardList = dataset.getShardList();
                long newestShardVersionForHost = 0;
                for(final ShardInfo shard : shardList) {
                    final long shardVersion = shard.getVersion();
                    if(shardVersion > newestShardVersionForHost) {
                        newestShardVersionForHost = shardVersion;
                    }
                }
                if(newestShardVersionForHost > datasetNewestShardMetadata.newestShardVersion) {
                    datasetNewestShardMetadata.newestShardVersion = newestShardVersionForHost;
                    datasetNewestShardMetadata.intFields = dataset.getIntFields();
                    datasetNewestShardMetadata.stringFields = dataset.getStringFields();
                }
                current.getShardList().addAll(shardList);
                current.getIntFields().addAll(dataset.getIntFields());
                current.getStringFields().addAll(dataset.getStringFields());
                current.getMetrics().addAll(dataset.getMetrics());
            }
        }

        // use the newest shard to disambiguate fields that have both String and Int types
        for(final Map.Entry<String, DatasetNewestShardMetadata> entry: datasetNameToMetadata.entrySet()) {
            final String datasetName = entry.getKey();
            final ImhotepClient.DatasetNewestShardMetadata newestMetadata = entry.getValue();
            final DatasetInfo datasetInfo = ret.get(datasetName);

            datasetInfo.getStringFields().removeAll(newestMetadata.intFields);
            datasetInfo.getIntFields().removeAll(newestMetadata.stringFields);

            // for the fields that still conflict in the newest shard, let the clients decide
            final Set<String> lastShardConflictingFields = Sets.intersection(Sets.newHashSet(newestMetadata.intFields), Sets.newHashSet(newestMetadata.stringFields));
            datasetInfo.getStringFields().addAll(lastShardConflictingFields);
            datasetInfo.getIntFields().addAll(lastShardConflictingFields);
        }
        return ret;
    }

    public List<String> getShardList(final String dataset) {
        return getShardList(dataset, new AcceptAllShardFilter());
    }

    public List<String> getShardList(final String dataset, final ShardFilter filterFunc) {
        final Map<Host, List<DatasetInfo>> shardListMap = getShardList();
        final SortedSet<String> set = new TreeSet<>();
        for (final List<DatasetInfo> datasetList : shardListMap.values()) {
            for (final DatasetInfo datasetInfo : datasetList) {
                for (final ShardInfo shard : datasetInfo.getShardList()) {
                    if (dataset.equals(shard.dataset) && filterFunc.accept(shard)) {
                        set.add(shard.shardId);
                    }
                }
            }
        }
        return Lists.newArrayList(set);
    }

    public List<ShardIdWithVersion> getShardListWithVersion(final String dataset, final ShardFilter filterFunc) {
        final Map<Host, List<DatasetInfo>> shardListMap = getShardList();

        final Map<String,Long> latestVersionMap = new HashMap<>();
        for (final List<DatasetInfo> datasetList : shardListMap.values()) {
            for (final DatasetInfo datasetInfo : datasetList) {
                if (!dataset.equals(datasetInfo.getDataset())) {
                    continue;
                }
                for (final ShardInfo shard : datasetInfo.getShardList()) {
                    if (filterFunc.accept(shard)) {
                        //is in time range, check version
                        if(!latestVersionMap.containsKey(shard.shardId) || latestVersionMap.get(shard.shardId) < shard.version) {
                            latestVersionMap.put(shard.shardId, shard.version);
                        }
                    }
                }
            }
        }

        final List<ShardIdWithVersion> ret = Lists.newArrayListWithCapacity(latestVersionMap.size());
        for (final Map.Entry<String, Long> e : latestVersionMap.entrySet()) {
            ret.add(new ShardIdWithVersion(e.getKey(), e.getValue()));
        }
        Collections.sort(ret);
        return ret;
    }

    /**
     * Returns a list of non-overlapping Imhotep shards for the specified dataset and time range.
     * Shards in the list are sorted chronologically.
     */
    public List<ShardIdWithVersion> findShardsForTimeRange(final String dataset, final DateTime start, final DateTime end) {
        // get shards intersecting with (start,end) time range
        final List<ShardIdWithVersion> shardsForTime = getShardListWithVersion(dataset, new DateRangeShardFilter(start, end));
        return removeIntersectingShards(shardsForTime, dataset, start);
    }

    // we are truncating the shard start point as part of removeIntersectingShards so we make a wrapper for the ShardIdWithVersion
    private static class ShardTruncatedStart {
        private final ShardIdWithVersion shard;
        private final DateTime start;
        private final DateTime end;
        private final long version;

        private ShardTruncatedStart(final ShardIdWithVersion shard, final DateTime start) {
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
    static List<ShardIdWithVersion> removeIntersectingShards(final List<ShardIdWithVersion> shardsForTime, final String dataset, final DateTime start) {
        // we have to limit shard start times to the requested start time to avoid
        // longer shards with the earlier start time taking precedence over newer smaller shards
        final Map<Integer, List<ShardTruncatedStart>> shardsForTimeTruncatedPerSubshard = new HashMap<>();
        for (final ShardIdWithVersion shard : shardsForTime) {
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
            if (DynamicIndexSubshardDirnameUtil.isValidShardId(shard.getShardId())) {
                subshardId = DynamicIndexSubshardDirnameUtil.parseSubshardIdFromShardId(shard.getShardId());
            } else {
                subshardId = 0;
            }
            shardsForTimeTruncatedPerSubshard
                    .computeIfAbsent(subshardId, (ignored) -> new ArrayList<>())
                    .add(new ShardTruncatedStart(shard, shardStart));
        }

        final List<ShardIdWithVersion> chosenShards = Lists.newArrayList();
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

        private Collection<String> requestedMetrics = Collections.emptyList();
        private int mergeThreadLimit = ImhotepRemoteSession.DEFAULT_MERGE_THREAD_LIMIT;
        private String username = "";
        private String clientName = "";
        private boolean optimizeGroupZeroLookups = false;
        private int socketTimeout = -1;
        private long localTempFileSizeLimit = -1;
        private long daemonTempFileSizeLimit = -1;
        private long sessionTimeout = -1;

        private List<ShardIdWithVersion> chosenShards = null;
        private List<String> shardsOverride = null;
        private boolean useNativeFTGS;

        public SessionBuilder(final String dataset, final DateTime start, final DateTime end) {
            this.dataset = dataset;
            this.start = start;
            this.end = end;
        }

        public SessionBuilder requestedMetrics(final Collection<String> newRequestedMetrics) {
            this.requestedMetrics = Lists.newArrayList(newRequestedMetrics);
            return this;
        }

        public SessionBuilder mergeThreadLimit(final int newMergeThreadLimit) {
            this.mergeThreadLimit = newMergeThreadLimit;
            return this;
        }
        @Deprecated
        public SessionBuilder priority(final int priority) {
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

        public SessionBuilder shardsOverride(final Collection<String> requiredShards) {
            this.shardsOverride = Lists.newArrayList(requiredShards);
            return this;
        }

        public SessionBuilder useNativeFtgs() {
            useNativeFTGS = true;
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
        public List<ShardIdWithVersion> getChosenShards() {
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
            final List<ShardIdWithVersion> chosenShards = getChosenShards();

            final List<Interval> timeIntervalsMissingShards = Lists.newArrayList();
            DateTime processedUpTo = start;
            for(final ShardIdWithVersion shard : chosenShards) {
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
            final List<String> chosenShardIDs = shardsOverride != null ? shardsOverride : ShardIdWithVersion.keepShardIds(getChosenShards());
            return getSessionForShards(dataset, chosenShardIDs, requestedMetrics, mergeThreadLimit, username, clientName,
                    optimizeGroupZeroLookups, socketTimeout, localTempFileSizeLimit, daemonTempFileSizeLimit, useNativeFTGS, sessionTimeout);
        }
    }

    /**
     * @deprecated replaced by {@link #sessionBuilder}().build()
     */
    @Deprecated
    public ImhotepSession getSession(final String dataset, final Collection<String> requestedShards) {
        return getSession(dataset, requestedShards, Collections.<String>emptyList(), ImhotepRemoteSession.DEFAULT_MERGE_THREAD_LIMIT, -1);
    }

    /**
     * @deprecated replaced by {@link #sessionBuilder}().build()
     */
    @Deprecated
    public ImhotepSession getSession(final String dataset, final Collection<String> requestedShards, final int socketTimeout) {
        return getSession(dataset, requestedShards, Collections.<String>emptyList(), ImhotepRemoteSession.DEFAULT_MERGE_THREAD_LIMIT, socketTimeout);
    }

    /**
     * @deprecated replaced by {@link #sessionBuilder}().build()
     */
    @Deprecated
    public ImhotepSession getSession(final String dataset, final Collection<String> requestedShards, final Collection<String> requestedMetrics) {
        return getSession(dataset, requestedShards, requestedMetrics, ImhotepRemoteSession.DEFAULT_MERGE_THREAD_LIMIT, -1);
    }

    /**
     * @deprecated replaced by {@link #sessionBuilder}().build()
     */
    @Deprecated
    public ImhotepSession getSession(final String dataset, final Collection<String> requestedShards, final Collection<String> requestedMetrics,
                                     final int mergeThreadLimit) {
        return getSession(dataset, requestedShards, requestedMetrics, mergeThreadLimit, -1);
    }

    /**
     * @deprecated replaced by {@link #sessionBuilder}().build()
     */
    @Deprecated
    public ImhotepSession getSession(final String dataset, final Collection<String> requestedShards, final Collection<String> requestedMetrics,
            final int mergeThreadLimit, final int priority) {
            return getSession(dataset, requestedShards, requestedMetrics, mergeThreadLimit, priority, ImhotepRemoteSession.getUsername(), false, -1);
    }

    /**
     * @deprecated replaced by {@link #sessionBuilder}().build()
     */
    @Deprecated
    public ImhotepSession getSession(final String dataset, final Collection<String> requestedShards, final Collection<String> requestedMetrics,
                                     final int mergeThreadLimit, final int priority, final int socketTimeout) {
        return getSession(dataset, requestedShards, requestedMetrics, mergeThreadLimit, priority, ImhotepRemoteSession.getUsername(), false, socketTimeout);
    }

    /**
     * @deprecated replaced by {@link #sessionBuilder}().build()
     */
    @Deprecated
    public ImhotepSession getSession(final String dataset, final Collection<String> requestedShards, final Collection<String> requestedMetrics,
                                     final int mergeThreadLimit, final int priority, final String username) {
        return getSession(dataset, requestedShards, requestedMetrics, mergeThreadLimit, priority, username, false, -1);
    }

    /**
     * @deprecated replaced by {@link #sessionBuilder}().build()
     */
    @Deprecated
    public ImhotepSession getSession(final String dataset, final Collection<String> requestedShards, final Collection<String> requestedMetrics,
                                     final int mergeThreadLimit, final int priority, final String username, final boolean optimizeGroupZeroLookups) {
        return getSession(dataset, requestedShards, requestedMetrics, mergeThreadLimit, priority, username, optimizeGroupZeroLookups, -1);
    }

    /**
     * @deprecated replaced by {@link #sessionBuilder}().build()
     */
    @Deprecated
    public ImhotepSession getSession(final String dataset, final Collection<String> requestedShards, final Collection<String> requestedMetrics,
                                     final int mergeThreadLimit, final int priority, final String username,
                                     final boolean optimizeGroupZeroLookups, final int socketTimeout) {

        return getSessionForShards(dataset, requestedShards, requestedMetrics, mergeThreadLimit, username, "", optimizeGroupZeroLookups, socketTimeout, -1, -1, false, 0);
    }

    private ImhotepSession getSessionForShards(final String dataset, final Collection<String> requestedShards, final Collection<String> requestedMetrics,
                                               final int mergeThreadLimit, final String username, final String clientName,
                                               final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                               final long localTempFileSizeLimit, final long daemonTempFileSizeLimit,
                                               final boolean useNativeFtgs, final long sessionTimeout) {

        if(requestedShards == null || requestedShards.isEmpty()) {
            throw new IllegalArgumentException("No shards");
        }
        int retries = 2;
        final AtomicLong localTempFileSizeBytesLeft = localTempFileSizeLimit > 0 ? new AtomicLong(localTempFileSizeLimit) : null;
        Exception error = null;
        while (retries > 0) {
            final String sessionId = UUID.randomUUID().toString();
            ImhotepRemoteSession[] remoteSessions = null;
            try {
                remoteSessions = internalGetSession(dataset, requestedShards, requestedMetrics, mergeThreadLimit, username, clientName, optimizeGroupZeroLookups, socketTimeout, sessionId, daemonTempFileSizeLimit, localTempFileSizeBytesLeft, useNativeFtgs, sessionTimeout);
            } catch (final Exception e) {
                error = e;
            }
            if (remoteSessions == null) {
                --retries;
                if (retries > 0) {
                    shardListReloader.run();
                }
                continue;
            }
            final InetSocketAddress[] nodes = new InetSocketAddress[remoteSessions.length];
            for (int i = 0; i < remoteSessions.length; i++) {
                nodes[i] = remoteSessions[i].getInetSocketAddress();
            }
            return new RemoteImhotepMultiSession(remoteSessions, sessionId, nodes, localTempFileSizeLimit, localTempFileSizeBytesLeft);
        }
        throw new RuntimeException("unable to open session", error);
    }

    private static class IncrementalEvaluationState {

        private final Map<String, ShardData> unprocessedShards;
        private final Multimap<Host, String> unprocessedShardsByHost;

        public IncrementalEvaluationState(final Map<String, ShardData> shards) {
            unprocessedShards = shards;

            unprocessedShardsByHost = HashMultimap.create();
            for (final Map.Entry<String, ShardData> entry : shards.entrySet()) {
                final String shardId = entry.getKey();
                for (final Pair<Host, Integer> pair : entry.getValue().hostToLoadedMetrics) {
                    final Host host = pair.getFirst();
                    unprocessedShardsByHost.put(host, shardId);
                }
                if (entry.getValue().hostToLoadedMetrics.isEmpty()) {
                    throw new IllegalStateException("no shards for host " + entry.getKey());
                }
            }
        }

        public synchronized List<String> getBatch(final Host host, final long maxDocs) {
            final List<String> result = new ArrayList<>();
            int docCount = 0;

            for (final String shard : unprocessedShardsByHost.get(host)) {
                if (docCount >= maxDocs) {
                    break;
                }

                final ShardData data = unprocessedShards.get(shard);
                assert data != null;

                result.add(shard);
                docCount += data.numDocs;
            }

            for (final String shard : result) {
                final ShardData data = unprocessedShards.remove(shard);
                for (final Pair<Host, Integer> pair : data.hostToLoadedMetrics) {
                    unprocessedShardsByHost.remove(pair.getFirst(), shard);
                }
            }

            return result;
        }
    }

    public void evaluateOnSessions(
            final SessionCallback callback,
            final String dataset,
            final Collection<String> requestedShards,
            final long maxDocsPerSession) {

        // construct
        Map<String, ShardData> shardMap = constructPotentialShardMap(dataset, Collections.<String>emptySet());
        shardMap = Maps.newHashMap(
                Maps.filterKeys(shardMap, Predicates.in(ImmutableSet.copyOf(
                        requestedShards))));

        final Set<Host> hosts = Sets.newTreeSet();
        for (final ShardData data : shardMap.values()) {
            for (final Pair<Host, Integer> pair : data.hostToLoadedMetrics) {
                hosts.add(pair.getFirst());
            }
        }

        final IncrementalEvaluationState state = new IncrementalEvaluationState(shardMap);

        final ExecutorService executor = Executors.newCachedThreadPool();
        final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
        final List<Callable<Void>> callables = Lists.newArrayList();
        final String sessionId = UUID.randomUUID().toString();
        for (final Host host : hosts) {
            callables.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        while (true) {
                            if (Thread.interrupted()) {
                                throw new InterruptedException();
                            }

                            final List<String> shards = state.getBatch(host, maxDocsPerSession);
                            if (shards.isEmpty()) {
                                break;
                            }
                            log.info("Processing " + shards.size() + " for " + host);

                            final ImhotepRemoteSession session = ImhotepRemoteSession.openSession(host.getHostname(),
                                    host.getPort(), dataset, shards, sessionId, 0);
                            callback.handle(session);
                        }
                        return null;
                    } catch (final Exception e) {
                        throw new Exception("failed to get results for host " + host, e);
                    }
                }
            });
        }

        try {
            for (final Callable<Void> callable : callables) {
                completionService.submit(callable);
            }

            for (int i = 0; i < callables.size(); i++) {
                final Future<?> future = completionService.take(); // to wait for completion
                future.get(); // to propagate exceptions
            }
        } catch (final ExecutionException e) {
            throw new RuntimeException("exception while executing operation", e);
        } catch (final InterruptedException e) {
            throw new RuntimeException("interrupted while waiting for operation", e);
        } finally {
            executor.shutdownNow();
        }
    }

    private static class ShardData {
        final int numDocs;
        final long highestVersion;
        final List<Pair<Host, Integer>> hostToLoadedMetrics;

        private ShardData(final int numDocs, final long highestVersion, final List<Pair<Host, Integer>> hostToLoadedMetrics) {
            this.numDocs = numDocs;
            this.highestVersion = highestVersion;
            this.hostToLoadedMetrics = hostToLoadedMetrics;
        }
    }

    // returns null on error
    private ImhotepRemoteSession[]
        internalGetSession(final String dataset,
                           final Collection<String> requestedShards,
                           final Collection<String> requestedMetrics,
                           final int mergeThreadLimit,
                           final String username,
                           final String clientName,
                           final boolean optimizeGroupZeroLookups,
                           final int socketTimeout,
                           @Nullable final String sessionId,
                           final long tempFileSizeLimit,
                           @Nullable final AtomicLong tempFileSizeBytesLeft,
                           final boolean useNativeFtgs,
                           final long sessionTimeout) {

        final ShardsAndDocCounts shardsAndDocCounts = buildShardRequestMap(dataset, requestedShards, requestedMetrics);
        final Map<Host, List<String>> shardRequestMap = shardsAndDocCounts.shardRequestMap;
        final Map<Host, Integer> hostsToDocCounts = shardsAndDocCounts.hostDocCounts;


        if (shardRequestMap.isEmpty()) {
            log.error("unable to find all of the requested shards in dataset " + dataset +
                      " (shard list = " + requestedShards + ")");
            return null;
        }

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

    private ShardsAndDocCounts buildShardRequestMap(final String dataset, final Collection<String> requestedShards, final Collection<String> requestedMetrics) {
        final Set<String> requestedMetricsSet = new HashSet<>(requestedMetrics);
        final Map<String, ShardData> shardMap = constructPotentialShardMap(dataset, requestedMetricsSet);

        boolean error = false;
        for (final String shard : requestedShards) {
            if (!shardMap.containsKey(shard)) {
                log.error("shard " + shard + " not found");
                error = true;
            }
        }

        if (error) {
            return new ShardsAndDocCounts(Maps.<Host, List<String>>newHashMap(), Maps.<Host, Integer>newHashMap());
        }

        final List<String> sortedShards = new ArrayList<>(requestedShards);
        Collections.sort(sortedShards, new Comparator<String>() {
            @Override
            public int compare(final String o1, final String o2) {
                final int c1 = shardMap.get(o1).numDocs;
                final int c2 = shardMap.get(o2).numDocs;
                return -(c1 < c2 ? -1 : c1 > c2 ? 1 : 0);
            }
        });

        final Map<Host, Integer> hostDocCounts = new HashMap<>();
        final Map<Host, List<String>> shardRequestMap = new TreeMap<>();
        for (final String shard : sortedShards) {
            final List<Pair<Host, Integer>> potentialHosts = shardMap.get(shard).hostToLoadedMetrics;
            int minHostDocCount = Integer.MAX_VALUE;
            int minHostLoadedMetricCount = 0;
            Host minHost = null;
            for (final Pair<Host, Integer> p : potentialHosts) {
                final Host host = p.getFirst();
                final int loadedMetricCount = p.getSecond();

                if (!hostDocCounts.containsKey(host)) {
                    hostDocCounts.put(host, 0);
                }
                if (loadedMetricCount > minHostLoadedMetricCount || hostDocCounts.get(host) < minHostDocCount) {
                    minHostDocCount = hostDocCounts.get(host);
                    minHostLoadedMetricCount = loadedMetricCount;
                    minHost = host;
                }
            }
            if (minHost == null) {
                throw new RuntimeException("something has gone horribly wrong");
            }

            if (!shardRequestMap.containsKey(minHost)) {
                shardRequestMap.put(minHost, new ArrayList<String>());
            }
            shardRequestMap.get(minHost).add(shard);
            hostDocCounts.put(minHost, hostDocCounts.get(minHost) + shardMap.get(shard).numDocs);
        }
        return new ShardsAndDocCounts(shardRequestMap, hostDocCounts);
    }

    /**
     * Given a dataset and a list of requested metrics, compute a map from shard IDs to lists of
     * (host, # of loaded metrics) pairs.
     *
     * @param dataset The dataset name
     * @param requestedMetricsSet The set of metrics whose loaded status should be counted
     * @return The resulting map
     */
    private Map<String, ShardData> constructPotentialShardMap(final String dataset, final Set<String> requestedMetricsSet) {
        final Map<String, ShardData> shardMap = Maps.newHashMap();
        final Map<Host, List<DatasetInfo>> shardListMap = getShardList();
        for (final Map.Entry<Host, List<DatasetInfo>> e : shardListMap.entrySet()) {
            final Host host = e.getKey();
            final List<DatasetInfo> shardList = e.getValue();
            for (final DatasetInfo datasetInfo : shardList) {
                if (!dataset.equals(datasetInfo.getDataset())) {
                    continue;
                }

                for (final ShardInfo shard : datasetInfo.getShardList()) {
                    if (!shardMap.containsKey(shard.shardId)) {
                        shardMap.put(shard.shardId, new ShardData(shard.numDocs, shard.version, new ArrayList<Pair<Host, Integer>>()));
                    } else {
                        final ShardData shardData = shardMap.get(shard.shardId);
                        final long highestKnownVersion = shardData.highestVersion;
                        if (highestKnownVersion < shard.version) {
                            // a newer version was found and all the previously encountered data for this shard should be removed
                            shardMap.put(shard.shardId, new ShardData(shard.numDocs, shard.version, new ArrayList<Pair<Host, Integer>>()));
                        } else if (highestKnownVersion > shard.version) {
                            continue; // this shard has an outdated version and should be skipped
                        } // else if (highestKnownVersion == shard.version) // just continue
                    }
                    final int loadedMetricsCount = Sets.intersection(requestedMetricsSet, new HashSet<>(shard.loadedMetrics)).size();
                    shardMap.get(shard.shardId).hostToLoadedMetrics.add(Pair.of(host, loadedMetricsCount));
                }
            }
        }
        return shardMap;
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
        return hostsSource.isLoadedDataSuccessfullyRecently() && shardListReloader.isLoadedDataSuccessfullyRecently();
    }

    public void addObserver(final Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    public void removeObserver(final Instrumentation.Observer observer) {
        instrumentation.removeObserver(observer);
    }
}
