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
 package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.protobuf.ByteString;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSModifiers;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.HasSessionId;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.imhotep.protobuf.AggregateStat;
import com.indeed.imhotep.protobuf.DocStat;
import com.indeed.imhotep.protobuf.FieldAggregateBucketRegroupRequest;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.MultiFTGSRequest;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.longs.LongIterators;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author jsgroth
 */
public class RemoteImhotepMultiSession extends AbstractImhotepMultiSession<ImhotepRemoteSession> implements HasSessionId {
    private static final Logger log = Logger.getLogger(RemoteImhotepMultiSession.class);

    private final InetSocketAddress[] nodes;

    private final long localTempFileSizeLimit;

    public RemoteImhotepMultiSession(final ImhotepRemoteSession[] sessions,
                                     final String sessionId,
                                     final InetSocketAddress[] nodes,
                                     final long localTempFileSizeLimit,
                                     final AtomicLong tempFileSizeBytesLeft,
                                     final String userName,
                                     final String clientName,
                                     final byte priority) {
        super(sessionId, sessions, tempFileSizeBytesLeft, userName, clientName, priority);

        this.nodes = nodes;
        this.localTempFileSizeLimit = localTempFileSizeLimit;
    }

    @Override
    public long[] getGroupStats(final String groupsName, final List<String> stat) throws ImhotepOutOfMemoryException {
        try(final GroupStatsIterator it = getGroupStatsIterator(groupsName, stat)) {
            return LongIterators.unwrap(it, it.getNumGroups());
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final String groupsName, final List<String> stat) throws ImhotepOutOfMemoryException {
        // there is two ways to create GroupStatsIterator in multisession:
        // create iterator over result of getGroupStats method or create merger for iterators.
        // In case of remote multisession we creating readers over socket streams.
        // It could be an issue if client is not reading stats till end.
        // But now, nobody uses this method (IQL uses getGroupStats(..))
        final GroupStatsIterator[] statsBuffer = new GroupStatsIterator[sessions.length];
        closeOnFailExecutor()
                .executeMemoryException(statsBuffer, session -> session.getGroupStatsIterator(groupsName, stat));

        if(statsBuffer.length == 1) {
            return statsBuffer[0];
        } else {
            return new GroupStatsIteratorCombiner(statsBuffer);
        }
    }

    @Override
    public FTGSIterator getFTGSIterator(final String groupsName, final FTGSParams params) throws ImhotepOutOfMemoryException {
        if (sessions.length == 1) {
            return sessions[0].getFTGSIterator(groupsName, params);
        }
        final FTGSIterator[] mergers = getFTGSIteratorSplits(groupsName, params);
        FTGSIterator interleaver = params.sorted ? new SortedFTGSInterleaver(mergers) : new UnsortedFTGSIterator(mergers);
        if (params.isTopTerms()) {
            interleaver = FTGSIteratorUtil.getTopTermsFTGSIterator(interleaver, params.termLimit, params.sortStat, params.statsSortOrder);
        } else if (params.isTermLimit()) {
            interleaver = new TermLimitedFTGSIterator(interleaver, params.termLimit);
        }
        return interleaver;
    }

    public FTGSIterator[] getFTGSIteratorSplits(final String groupsName, final String[] intFields, final String[] stringFields, final long termLimit) throws ImhotepOutOfMemoryException {
        if (sessions.length == 1) {
            final FTGSIterator result = sessions[0].getFTGSIterator(groupsName, intFields, stringFields, termLimit, null);
            return new FTGSIterator[] {result};
        }
        return getFTGSIteratorSplits(groupsName, new FTGSParams(intFields, stringFields, termLimit, -1, true, null, StatsSortOrder.UNDEFINED));
    }

    private FTGSIterator[] getFTGSIteratorSplits(final String groupsName, final FTGSParams params) throws ImhotepOutOfMemoryException {
        checkSplitParams(sessions.length);
        final Pair<Integer, ImhotepRemoteSession>[] indexesAndSessions = new Pair[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            indexesAndSessions[i] = Pair.of(i, sessions[i]);
        }
        final FTGSIterator[] mergers = new FTGSIterator[sessions.length];
        // We don't need sorted for top terms case since mergers will be passed
        // in FTGSIteratorUtil.getTopTermsFTGSIterator anyway
        final FTGSParams perSplitParams = params.isTopTerms() ? params.unsortedCopy() : params.copy();
        this.closeOnFailExecutor()
                .lockCPU(false)
                .executeMemoryException(mergers, indexesAndSessions, indexSessionPair -> {
                    final ImhotepRemoteSession session = indexSessionPair.getSecond();
                    final int index = indexSessionPair.getFirst();
                    return session.mergeFTGSSplit(groupsName, perSplitParams, nodes, index);
                });
        return mergers;
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final String groupsName, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        if (sessions.length == 1) {
            return sessions[0].getSubsetFTGSIterator(groupsName, intFields, stringFields, stats);
        }
        final FTGSIterator[] mergers = getSubsetFTGSIteratorSplits(groupsName, intFields, stringFields, stats);
        return new SortedFTGSInterleaver(mergers);
    }

    private FTGSIterator[] getSubsetFTGSIteratorSplits(final String groupsName, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        final Pair<Integer, ImhotepRemoteSession>[] indexesAndSessions = new Pair[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            indexesAndSessions[i] = Pair.of(i, sessions[i]);
        }
        final FTGSIterator[] mergers = new FTGSIterator[sessions.length];
        this.closeOnFailExecutor()
                .lockCPU(false)
                .executeMemoryException(mergers, indexesAndSessions, indexSessionPair -> {
                    final ImhotepRemoteSession session = indexSessionPair.getSecond();
                    final int index = indexSessionPair.getFirst();
                    return session.mergeSubsetFTGSSplit(groupsName, intFields, stringFields, stats, nodes, index);
                });
        return mergers;
    }

    @Override
    public GroupStatsIterator getDistinct(final String groupsName, final String field, final boolean isIntField) {
        if (sessions.length == 1) {
            return sessions[0].getDistinct(field, isIntField);
        }

        final Pair<Integer, ImhotepRemoteSession>[] indexesAndSessions = new Pair[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            indexesAndSessions[i] = Pair.of(i, sessions[i]);
        }
        final GroupStatsIterator[] mergers = new GroupStatsIterator[sessions.length];
        this.closeOnFailExecutor()
                .lockCPU(false)
                .executeRuntimeException(mergers, indexesAndSessions, indexSessionPair -> {
                    final ImhotepRemoteSession session = indexSessionPair.getSecond();
                    final int index = indexSessionPair.getFirst();
                    return session.mergeDistinctSplit(groupsName, field, isIntField, nodes, index);
                });
        return new GroupStatsIteratorCombiner(mergers);
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final GroupMultiRemapRuleSender ruleSender =
                GroupMultiRemapRuleSender.createFromRules(
                        Arrays.asList(rawRules).iterator(),
                        sessions.length > 1);
        return regroupWithRuleSender(regroupParams, ruleSender, errorOnCollisions);
    }

    public int regroupWithRuleSender(final RegroupParams regroupParams, final GroupMultiRemapRuleSender ruleSender, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        executor().executeMemoryException(integerBuf, session -> session.regroupWithSender(regroupParams, ruleSender, errorOnCollisions));
        return Collections.max(Arrays.asList(integerBuf));
    }

    /**
     * Returns the number of bytes written to the temp files for this session locally.
     * Returns -1 if tempFileSizeBytesLeft was set to null.
     */
    public long getTempFilesBytesWritten() {
        if(tempFileSizeBytesLeft == null || localTempFileSizeLimit <= 0) {
            return -1;
        }
        return localTempFileSizeLimit - tempFileSizeBytesLeft.get();
    }

    // Combination rules are different for remote sessions vs what is done in AbstractImhotepMultiSession for local sessions
    @Override
    protected PerformanceStats combinePerformanceStats(boolean reset, PerformanceStats[] stats) {
        if(stats == null) {
            return null;
        }
        final PerformanceStats.Builder builder = PerformanceStats.builder();
        for (final PerformanceStats stat : stats) {
            if(stat != null) {
                builder.add(stat);
            }
        }
        return builder.build();
    }

    @Override
    public void stringOrRegroup(final RegroupParams regroupParams, String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        final ImhotepRequestSender request = createSender(sessions[0].buildStringOrRegroupRequest(regroupParams, field, terms, targetGroup, negativeGroup, positiveGroup));
        executor().executeMemoryExceptionVoid(nullBuf, session -> session.sendVoidRequest(request));
    }

    @Override
    public void intOrRegroup(final RegroupParams regroupParams, String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        final ImhotepRequestSender request = createSender(sessions[0].buildIntOrRegroupRequest(regroupParams, field, terms, targetGroup, negativeGroup, positiveGroup));
        executor().executeMemoryExceptionVoid(nullBuf, session -> session.sendVoidRequest(request));
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        final ImhotepRequestSender request = createSender(sessions[0].buildGroupRemapRequest(regroupParams, fromGroups, toGroups, filterOutNotTargeted));
        executor().executeMemoryException(integerBuf, session -> session.sendRegroupRequest(request));
        return Collections.max(Arrays.asList(integerBuf));
    }

    public static class PerSessionFTGSInfo {
        private final RemoteImhotepMultiSession session;
        private final Runnable synchronizeCallback;
        private final String field;
        @Nullable
        private final List<List<String>> stats;
        private final String groupsName;
        private final String outputGroupsName; // For field aggregate bucket

        public PerSessionFTGSInfo(final ImhotepSession session, final String field, @Nullable final List<List<String>> stats) {
            this(session, field, stats, ImhotepSession.DEFAULT_GROUPS, ImhotepSession.DEFAULT_GROUPS);
        }

        /**
         * This constructor has sharp edges.
         * It takes an ImhotepSession for ease of use because ImhotepClient doesn't return a concrete type
         *
         * @throws IllegalArgumentException if session is not a RemoteImhotepMultiSession or AsynchronousRemoteImhotepMultiSession
         */
        public PerSessionFTGSInfo(final ImhotepSession session, final String field, @Nullable final List<List<String>> stats, final String groupsName, final String outputGroupsName) {
            this.groupsName = groupsName;
            this.outputGroupsName = outputGroupsName;
            if (session instanceof RemoteImhotepMultiSession) {
                this.session = (RemoteImhotepMultiSession) session;
                this.synchronizeCallback = () -> {};
            } else if (session instanceof BatchRemoteImhotepMultiSession) {
                final BatchRemoteImhotepMultiSession batchRemoteImhotepMultiSession = (BatchRemoteImhotepMultiSession) session;
                this.session = batchRemoteImhotepMultiSession.remoteImhotepMultiSession;
                this.synchronizeCallback = batchRemoteImhotepMultiSession::executeBatchNoMemoryException;
            } else {
                throw new IllegalArgumentException("Can only use RemoteImhotepMultiSession::multiFtgs on RemoteImhotepMultiSession/AsynchronousRemoteImhotepMultiSession/BatchRemoteImhotepMultiSession instances.");
            }
            this.field = field;
            this.stats = stats;
        }
    }

    /**
     * The data in the returned GroupStatsIterator is laid out as
     * (group = g, filter = f, assuming 3 filters)
     * g0 f0, g0 f1, g0 f2, g1 f0, g1 f1, g1 f2, ...
     */
    public static GroupStatsIterator aggregateDistinct(
            final List<PerSessionFTGSInfo> sessionsWithFields,
            final List<AggregateStatTree> filters,
            final boolean isIntField
    ) throws ImhotepOutOfMemoryException {
        return aggregateDistinct(sessionsWithFields, filters, Collections.nCopies(filters.size(), 1), isIntField, null);
    }

    /**
     * The data in the returned GroupStatsIterator is laid out as
     * (group = g, filter = f, assuming 3 filters)
     * g0 f0, g0 f1, g0 f2, g1 f0, g1 f1, g1 f2, ...
     */
    public static GroupStatsIterator aggregateDistinct(
            final List<PerSessionFTGSInfo> sessionsWithFields,
            final List<AggregateStatTree> filters,
            final List<Integer> windowSizes,
            final boolean isIntField,
            @Nullable final int[] parentGroups
    ) throws ImhotepOutOfMemoryException {
        final MultiFTGSRequest.Builder builder = MultiFTGSRequest.newBuilder();
        final List<RemoteImhotepMultiSession> remoteSessions = processSessionFields(sessionsWithFields, builder);
        builder
                .addAllFilter(AggregateStatTree.allAsList(filters))
                .setIsIntField(isIntField);

        final Pair<Integer, HostAndPort>[] indexedServers = indexedServers(builder.getNodesList());
        builder.addAllWindowSize(windowSizes);
        if (parentGroups != null) {
            builder.setParentGroups(ByteString.copyFrom(ImhotepProtobufShipping.runLengthEncode(parentGroups)));
        }
        final MultiFTGSRequest baseRequest = builder.build();

        final Closer closeOnFailCloser = Closer.create();
        try {
            final GroupStatsIterator[] subCounts = new GroupStatsIterator[indexedServers.length];
            closeOnFailCloser.register(Closeables2.forArray(log, subCounts));

            final AtomicLong tempFileSizeBytesLeft = getTempFileSizeBytesLeft(sessionsWithFields);
            final String concatenatedSessionIds = getConcatenatedSessionIds(sessionsWithFields);

            // Arbitrarily use the executor for the first session.
            // Still allows a human to understand and avoids making global state executors
            // or passing in an executor.
            remoteSessions.get(0)
                    .executor()
                    .lockCPU(false)
                    .execute(subCounts, indexedServers, pair -> {
                final int index = pair.getFirst();
                final HostAndPort hostAndPort = pair.getSecond();
                // Definitely don't close this session
                //noinspection resource,IOResourceOpenedButNotSafelyClosed
                final ImhotepRemoteSession remoteSession = new ImhotepRemoteSession(hostAndPort.getHost(), hostAndPort.getPort(), concatenatedSessionIds, tempFileSizeBytesLeft, ImhotepRemoteSession.DEFAULT_SOCKET_TIMEOUT);
                final MultiFTGSRequest proto = MultiFTGSRequest.newBuilder(baseRequest).setSplitIndex(index).build();
                return remoteSession.aggregateDistinct(proto);
            });

            return new GroupStatsIteratorCombiner(subCounts);
        } catch (Throwable t) {
            Closeables2.closeQuietly(closeOnFailCloser, log);
            if (t instanceof ExecutionException) {
                Throwables.propagateIfInstanceOf(t.getCause(), ImhotepOutOfMemoryException.class);
            }
            throw Throwables.propagate(t);
        }
    }

    public static FTGAIterator multiFtgs(
            final List<PerSessionFTGSInfo> sessionsWithFields,
            final List<AggregateStatTree> selects,
            final List<AggregateStatTree> filters,
            final boolean isIntField,
            final long termLimit,
            final int sortStat,
            final boolean sorted,
            final StatsSortOrder statsSortOrder
    ) throws ImhotepOutOfMemoryException {
        return multiFtgs(
                sessionsWithFields,
                AggregateStatTree.allAsList(selects),
                AggregateStatTree.allAsList(filters),
                isIntField,
                termLimit,
                sortStat,
                sorted,
                statsSortOrder,
                0,
                1
        );
    }

    // There's a bit of trickiness going on here.
    //
    // We need to get the same term from all sessions to the same node.
    // This is accomplished by using a number of splits determined by the union of the set of servers involved in each session.
    // Every server then requests its 1/N split of the terms.
    //
    // Within each node, we need to get the same term from all of the inputs into the same merge thread split.
    // We do this by using the same number of "local splits" (numLocalSplits parameter here) across all of the different
    // sessions we're requesting from (1 call to partialMergeFTGSSplit per session).
    //
    // Those then get combined by the thing that handles the results of this, index-for-index zipping across all of the datasets.
    //
    // With numReplica > 1, each daemon will persist the resulted merged ftgs split so that it can return the same multiFtgs for
    // all 0 <= replicaId < numReplica without re-calculating them.
    public static FTGAIterator multiFtgs(
            final List<PerSessionFTGSInfo> sessionsWithFields,
            final List<AggregateStat> selects,
            final List<AggregateStat> filters,
            final boolean isIntField,
            final long termLimit,
            final int sortStat,
            final boolean sorted,
            final StatsSortOrder statsSortOrder,
            final int replicaId,
            final int numReplica
    ) throws ImhotepOutOfMemoryException {
        final MultiFTGSRequest.Builder builder = MultiFTGSRequest.newBuilder();
        final List<RemoteImhotepMultiSession> remoteSessions = processSessionFields(sessionsWithFields, builder);

        builder
                .addAllSelect(selects)
                .addAllFilter(filters)
                .setIsIntField(isIntField)
                .setTermLimit(termLimit)
                .setSortStat(sortStat)
                .setSortedFTGS(sorted)
                .setStatsSortOrder(statsSortOrder)
                .setReplicaId(replicaId)
                .setNumReplica(numReplica);

        final Pair<Integer, HostAndPort>[] indexedServers = indexedServers(builder.getNodesList());

        final MultiFTGSRequest baseRequest = builder.build();

        final FTGAIterator[] subIterators = new FTGAIterator[indexedServers.length];

        final Closer closeOnFailCloser = Closer.create();
        closeOnFailCloser.register(Closeables2.forArray(log, subIterators));
        try {
            final AtomicLong tempFileSizeBytesLeft = getTempFileSizeBytesLeft(sessionsWithFields);
            final String concatenatedSessionIds = getConcatenatedSessionIds(sessionsWithFields);

            // Arbitrarily use the executor for the first session.
            // Still allows a human to understand and avoids making global state executors
            // or passing in an executor.
            remoteSessions.get(0)
                    .executor()
                    .lockCPU(false)
                    .executeMemoryException(subIterators, indexedServers, pair -> {
                        final int index = pair.getFirst();
                        final HostAndPort hostAndPort = pair.getSecond();
                        // Definitely don't close this session
                        //noinspection IOResourceOpenedButNotSafelyClosed
                        final ImhotepRemoteSession remoteSession = new ImhotepRemoteSession(hostAndPort.getHost(), hostAndPort.getPort(), concatenatedSessionIds, tempFileSizeBytesLeft, ImhotepRemoteSession.DEFAULT_SOCKET_TIMEOUT);
                        final MultiFTGSRequest proto = MultiFTGSRequest.newBuilder(baseRequest).setSplitIndex(index).build();
                        return remoteSession.multiFTGS(proto);
                    });
        } catch (final Throwable t) {
            Closeables2.closeQuietly(closeOnFailCloser, log);
            throw t;
        }

        final FTGSModifiers modifiers = new FTGSModifiers(termLimit, sortStat, sorted, statsSortOrder);

        final FTGAIterator interleaver;
        if (sorted) {
            //noinspection resource
            interleaver = new SortedFTGAInterleaver(subIterators);
        } else {
            //noinspection resource
            interleaver = new UnsortedFTGAIterator(subIterators);
        }

        return modifiers.wrap(interleaver);
    }

    // In each node, we first pull FTGA for the metric from all the nodes, calculate the output group for each term/group pair
    // using the A of FTGA and bucket parameters, and then regroup docs.
    //
    // Since each node will require exactly the same FTGA, we'll use replicaId/numReplica args of multiFtgs method.
    public static int aggregateBucketRegroup(
            final List<PerSessionFTGSInfo> sessionsWithFields,
            final AggregateStatTree metric,
            final boolean isIntField,
            final long min,
            final long max,
            final long interval,
            final boolean excludeGutters,
            final boolean withDefault
    ) throws ImhotepOutOfMemoryException {
        final FieldAggregateBucketRegroupRequest.Builder builder = FieldAggregateBucketRegroupRequest.newBuilder();
        final Pair<List<RemoteImhotepMultiSession>, Map<HostAndPort, List<String>>> remoteSessionsAndSessionIds = processSessionFields(sessionsWithFields, builder);
        final List<RemoteImhotepMultiSession> remoteSessions = remoteSessionsAndSessionIds.getFirst();
        final Map<HostAndPort, List<String>> sessionIdsPerHost = remoteSessionsAndSessionIds.getSecond();
        builder
                .addAllMetric(metric.asList())
                .setIsIntField(isIntField)
                .setMin(min)
                .setMax(max)
                .setInterval(interval)
                .setExcludeGutters(excludeGutters)
                .setWithDefault(withDefault);

        final Pair<Integer, HostAndPort>[] indexedServers = indexedServers(builder.getNodesList());
        final Integer[] intBuffer = new Integer[indexedServers.length];

        final FieldAggregateBucketRegroupRequest requestBase = builder.build();
        final AtomicLong tempFileSizeBytesLeft = getTempFileSizeBytesLeft(sessionsWithFields);
        final String concatenatedSessionIds = getConcatenatedSessionIds(sessionsWithFields);

        // Arbitrarily use the executor for the first session.
        // Still allows a human to understand and avoids making global state executors
        // or passing in an executor.
        remoteSessions.get(0)
                .executor()
                .lockCPU(false) // Just send request and receive response.
                .executeMemoryException(intBuffer, indexedServers, pair -> {
                    // Definitely don't close this session
                    //noinspection IOResourceOpenedButNotSafelyClosed
                    final ImhotepRemoteSession remoteSession = new ImhotepRemoteSession(pair.getSecond().getHost(), pair.getSecond().getPort(), concatenatedSessionIds, tempFileSizeBytesLeft, ImhotepRemoteSession.DEFAULT_SOCKET_TIMEOUT);
                    final FieldAggregateBucketRegroupRequest request = FieldAggregateBucketRegroupRequest.newBuilder(requestBase)
                            .addAllSessionId(sessionIdsPerHost.get(pair.getSecond()))
                            .setMyIndex(pair.getFirst())
                            .build();
                    return remoteSession.fieldAggregateBucketRegroup(request);
                });
        return Collections.max(Arrays.asList(intBuffer));
    }

    // This gives a conservative choice for when we have multiple sessions
    // with separate tempFileSizeBytesLeft, and also will behave correctly
    // once we fix it so that multiple sessions from the same IQL2 query
    // share a single AtomicLong.
    private static AtomicLong getTempFileSizeBytesLeft(final List<PerSessionFTGSInfo> sessionsWithFields) {
        return sessionsWithFields
                .stream()
                .map(x -> x.session.tempFileSizeBytesLeft)
                .filter(Objects::nonNull)
                .min(Comparator.comparingLong(AtomicLong::get))
                .orElse(null);
    }

    // This will not affect semantics on the other side but allows
    // properly tracing and logging things in useful ways.
    private static String getConcatenatedSessionIds(final List<PerSessionFTGSInfo> sessionsWithFields) {
        return sessionsWithFields
                        .stream()
                        .map(x -> x.session.getSessionId())
                        .distinct()
                        .sorted()
                        .collect(Collectors.joining(","));
    }

    @Nonnull
    private static Pair<Integer, HostAndPort>[] indexedServers(final List<HostAndPort> nodesList) {
        final Pair<Integer, HostAndPort>[] indexedServers = new Pair[nodesList.size()];
        for (int i = 0; i < nodesList.size(); i++) {
            HostAndPort hostAndPort = nodesList.get(i);
            indexedServers[i] = new Pair<>(i, hostAndPort);
        }
        return indexedServers;
    }

    // Adds nodes and sessionInfos to the given builder, and extracts the list of imhotep sessions
    private static List<RemoteImhotepMultiSession> processSessionFields(final List<PerSessionFTGSInfo> sessionsWithFields, final MultiFTGSRequest.Builder builder) {
        final List<RemoteImhotepMultiSession> remoteSessions = new ArrayList<>();
        final Set<HostAndPort> allNodes = new HashSet<>();

        for (final PerSessionFTGSInfo perSessionFTGSInfo : sessionsWithFields) {
            perSessionFTGSInfo.synchronizeCallback.run();

            final RemoteImhotepMultiSession session = perSessionFTGSInfo.session;
            remoteSessions.add(session);
            final String fieldName = perSessionFTGSInfo.field;
            final List<HostAndPort> nodes = Arrays.stream(session.nodes).map(input ->
                    HostAndPort.newBuilder()
                            .setHost(input.getHostName())
                            .setPort(input.getPort())
                            .build()
            ).collect(Collectors.toList());
            allNodes.addAll(nodes);
            final MultiFTGSRequest.MultiFTGSSession.Builder sessionBuilder = builder.addSessionInfoBuilder()
                    .setSessionId(session.getSessionId())
                    .addAllNodes(nodes)
                    .setField(fieldName);

            if (perSessionFTGSInfo.stats != null) {
                sessionBuilder.addAllStats(perSessionFTGSInfo.stats.stream().map(x -> DocStat.newBuilder().addAllStat(x).build()).collect(Collectors.toList()));
                sessionBuilder.setHasStats(true);
            }

            sessionBuilder.setGroupsName(perSessionFTGSInfo.groupsName);
        }

        final List<HostAndPort> allNodesList = Lists.newArrayList(allNodes);
        builder.addAllNodes(allNodesList);
        return remoteSessions;
    }

    // Adds nodes and sessionInfos to the given builder, and extracts the list of imhotep sessions and session id per host
    private static Pair<List<RemoteImhotepMultiSession>, Map<HostAndPort, List<String>>> processSessionFields(final List<PerSessionFTGSInfo> sessionsWithFields, final FieldAggregateBucketRegroupRequest.Builder builder) {
        final List<RemoteImhotepMultiSession> remoteSessions = new ArrayList<>();
        final Map<HostAndPort, List<String>> sessionsPerNode = new HashMap<>();

        for (final PerSessionFTGSInfo perSessionFTGSInfo : sessionsWithFields) {
            perSessionFTGSInfo.synchronizeCallback.run();

            final RemoteImhotepMultiSession session = perSessionFTGSInfo.session;
            remoteSessions.add(session);
            final String fieldName = perSessionFTGSInfo.field;
            final List<HostAndPort> nodes = Arrays.stream(session.nodes).map(input ->
                    HostAndPort.newBuilder()
                            .setHost(input.getHostName())
                            .setPort(input.getPort())
                            .build()
            ).collect(Collectors.toList());
            for (final HostAndPort node : nodes) {
                sessionsPerNode.computeIfAbsent(node, ignored -> new ArrayList<>()).add(session.getSessionId());
            }
            final FieldAggregateBucketRegroupRequest.FieldAggregateBucketRegroupSession.Builder sessionBuilder = builder.addSessionInfoBuilder()
                    .setSessionId(session.getSessionId())
                    .addAllNodes(nodes)
                    .setField(fieldName);

            if (perSessionFTGSInfo.stats != null) {
                sessionBuilder.addAllStats(perSessionFTGSInfo.stats.stream().map(x -> DocStat.newBuilder().addAllStat(x).build()).collect(Collectors.toList()));
                sessionBuilder.setHasStats(true);
            }

            sessionBuilder.setInputGroup(perSessionFTGSInfo.groupsName);
            sessionBuilder.setOutputGroup(perSessionFTGSInfo.outputGroupsName);
        }

        final List<HostAndPort> allNodesList = Lists.newArrayList(sessionsPerNode.keySet());
        builder.addAllNodes(allNodesList);
        return Pair.of(remoteSessions, sessionsPerNode);
    }

    private RequestTools.ImhotepRequestSender createSender(final ImhotepRequest request) {
        if (sessions.length == 1) {
            return new RequestTools.ImhotepRequestSender.Simple(request);
        } else {
            return RequestTools.ImhotepRequestSender.Cached.create(request);
        }
    }

    <T> T processImhotepBatchRequest(final List<ImhotepCommand> firstCommands, final ImhotepCommand<T> lastCommand ) throws ImhotepOutOfMemoryException {
        final T[] buffer = (T[]) Array.newInstance(lastCommand.getResultClass(), sessions.length);
        closeIfCloseableOnFailExecutor().executeMemoryException(buffer, session -> session.sendImhotepBatchRequest(firstCommands, lastCommand));
        return lastCommand.combine(Arrays.asList(buffer));
    }
}