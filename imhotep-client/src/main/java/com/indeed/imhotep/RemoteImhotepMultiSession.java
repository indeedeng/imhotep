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
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSModifiers;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.HasSessionId;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.MultiFTGSRequest;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.longs.LongIterators;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
                                     final String clientName) {
        super(sessionId, sessions, tempFileSizeBytesLeft, userName, clientName);

        this.nodes = nodes;
        this.localTempFileSizeLimit = localTempFileSizeLimit;
    }

    @Override
    public long[] getGroupStats(final int stat) {
        try(final GroupStatsIterator it = getGroupStatsIterator(stat)) {
            return LongIterators.unwrap(it, it.getNumGroups());
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final int stat) {
        // there is two ways to create GroupStatsIterator in multisession:
        // create iterator over result of getGroupStats method or create merger for iterators.
        // In case of remote multisession we creating readers over socket streams.
        // It could be an issue if client is not reading stats till end.
        // But now, nobody uses this method (IQL uses getGroupStats(..))
        final GroupStatsIterator[] statsBuffer = new GroupStatsIterator[sessions.length];
        executeRuntimeException(statsBuffer, new ThrowingFunction<ImhotepRemoteSession, GroupStatsIterator>() {
            @Override
            public GroupStatsIterator apply(final ImhotepRemoteSession session) {
                return session.getGroupStatsIterator(stat);
            }
        });

        if(statsBuffer.length == 1) {
            return statsBuffer[0];
        } else {
            return new GroupStatsIteratorCombiner(statsBuffer);
        }
    }

    @Override
    public FTGSIterator getFTGSIterator(final FTGSParams params) {
        if (sessions.length == 1) {
            return sessions[0].getFTGSIterator(params);
        }
        final FTGSIterator[] mergers = getFTGSIteratorSplits(params);
        FTGSIterator interleaver = params.sorted ? new SortedFTGSInterleaver(mergers) : new UnsortedFTGSIterator(mergers);
        if (params.isTopTerms()) {
            interleaver = FTGSIteratorUtil.getTopTermsFTGSIterator(interleaver, params.termLimit, params.sortStat);
        } else if (params.isTermLimit()) {
            interleaver = new TermLimitedFTGSIterator(interleaver, params.termLimit);
        }
        return interleaver;
    }

    public FTGSIterator[] getFTGSIteratorSplits(final String[] intFields, final String[] stringFields, final long termLimit) {
        if (sessions.length == 1) {
            final FTGSIterator result = sessions[0].getFTGSIterator(intFields, stringFields, termLimit);
            return new FTGSIterator[] {result};
        }
        return getFTGSIteratorSplits(new FTGSParams(intFields, stringFields, termLimit, -1, true));
    }

    private FTGSIterator[] getFTGSIteratorSplits(final FTGSParams params) {
        checkSplitParams(sessions.length);
        final Pair<Integer, ImhotepRemoteSession>[] indexesAndSessions = new Pair[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            indexesAndSessions[i] = Pair.of(i, sessions[i]);
        }
        final FTGSIterator[] mergers = new FTGSIterator[sessions.length];
        final Closer closer = Closer.create();
        closer.register(Closeables2.forArray(log, mergers));
        try {
            // We don't need sorted for top terms case since mergers will be passed
            // in FTGSIteratorUtil.getTopTermsFTGSIterator anyway
            final FTGSParams perSplitParams = params.isTopTerms() ? params.unsortedCopy() : params.copy();
            execute(mergers, indexesAndSessions, false, new ThrowingFunction<Pair<Integer, ImhotepRemoteSession>, FTGSIterator>() {
                public FTGSIterator apply(final Pair<Integer, ImhotepRemoteSession> indexSessionPair) {
                    final ImhotepRemoteSession session = indexSessionPair.getSecond();
                    final int index = indexSessionPair.getFirst();
                    return session.mergeFTGSSplit(perSplitParams, nodes, index);
                }
            });
        } catch (final Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }
        return mergers;
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        if (sessions.length == 1) {
            return sessions[0].getSubsetFTGSIterator(intFields, stringFields);
        }
        final FTGSIterator[] mergers = getSubsetFTGSIteratorSplits(intFields, stringFields);
        return new SortedFTGSInterleaver(mergers);
    }

    private FTGSIterator[] getSubsetFTGSIteratorSplits(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        final Pair<Integer, ImhotepRemoteSession>[] indexesAndSessions = new Pair[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            indexesAndSessions[i] = Pair.of(i, sessions[i]);
        }
        final FTGSIterator[] mergers = new FTGSIterator[sessions.length];
        final Closer closer = Closer.create();
        closer.register(Closeables2.forArray(log, mergers));
        try {
            execute(mergers, indexesAndSessions, false, new ThrowingFunction<Pair<Integer, ImhotepRemoteSession>, FTGSIterator>() {
                public FTGSIterator apply(final Pair<Integer, ImhotepRemoteSession> indexSessionPair) {
                    final ImhotepRemoteSession session = indexSessionPair.getSecond();
                    final int index = indexSessionPair.getFirst();
                    return session.mergeSubsetFTGSSplit(intFields, stringFields, nodes, index);
                }
            });
        } catch (final Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }
        return mergers;
    }

    @Override
    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        if (sessions.length == 1) {
            return sessions[0].getDistinct(field, isIntField);
        }

        final Pair<Integer, ImhotepRemoteSession>[] indexesAndSessions = new Pair[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            indexesAndSessions[i] = Pair.of(i, sessions[i]);
        }
        final GroupStatsIterator[] mergers = new GroupStatsIterator[sessions.length];
        final Closer closer = Closer.create();
        closer.register(Closeables2.forArray(log, mergers));
        try {
            execute(mergers, indexesAndSessions, false, new ThrowingFunction<Pair<Integer, ImhotepRemoteSession>, GroupStatsIterator>() {
                public GroupStatsIterator apply(final Pair<Integer, ImhotepRemoteSession> indexSessionPair) {
                    final ImhotepRemoteSession session = indexSessionPair.getSecond();
                    final int index = indexSessionPair.getFirst();
                    return session.mergeDistinctSplit(field, isIntField, nodes, index);
                }
            });
        } catch (final Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }

        return new GroupStatsIteratorCombiner(mergers);
    }

    // Overrides the AbstractImhotepMultiSession implementation to avoid each sub-session constructing a separate copy
    // of the rules protobufs using too much RAM.
    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final GroupMultiRemapMessage[] groupMultiRemapMessages = new GroupMultiRemapMessage[rawRules.length];
        for(int i = 0; i < rawRules.length; i++) {
            groupMultiRemapMessages[i] = ImhotepClientMarshaller.marshal(rawRules[i]);
        }

        return regroupWithProtos(groupMultiRemapMessages, errorOnCollisions);
    }

    @Override
    public int regroupWithProtos(final GroupMultiRemapMessage[] rawRuleMessages, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepRemoteSession, Integer>() {
            @Override
            public Integer apply(final ImhotepRemoteSession session) throws ImhotepOutOfMemoryException{
                return session.regroupWithProtos(rawRuleMessages, errorOnCollisions);
            }
        });

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
    public void stringOrRegroup(String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = sessions[0].buildStringOrRegroupRequest(field, terms, targetGroup, negativeGroup, positiveGroup);
        executeMemoryException(nullBuf, session -> {
            session.sendVoidRequest(request);
            return null;
        });
    }

    @Override
    public void intOrRegroup(String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = sessions[0].buildIntOrRegroupRequest(field, terms, targetGroup, negativeGroup, positiveGroup);
        executeMemoryException(nullBuf, session -> {
            session.sendVoidRequest(request);
            return null;
        });
    }

    @Override
    public int regroup(final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = sessions[0].buildGroupRemapRequest(fromGroups, toGroups, filterOutNotTargeted);
        executeMemoryException(integerBuf, session -> session.sendRegroupRequest(request));

        return Collections.max(Arrays.asList(integerBuf));
    }

    public static class SessionField {
        private final RemoteImhotepMultiSession session;
        private final String field;

        /**
         * TODO: This exception seems a little funky?
         * @throws IllegalArgumentException if session is not a RemoteImhotepMultiSession
         */
        public SessionField(final ImhotepSession session, final String field) {
            if (!(session instanceof RemoteImhotepMultiSession)) {
                throw new IllegalArgumentException("Can only use RemoteImhotepMultiSession::multiFtgs on RemoteImhotepMultiSession instances.");
            }
            this.session = (RemoteImhotepMultiSession) session;
            this.field = field;
        }
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
    public static FTGAIterator multiFtgs(
            final List<SessionField> sessionsWithFields,
            final List<AggregateStatTree> selects,
            final List<AggregateStatTree> filters,
            final boolean isIntField,
            final long termLimit,
            final int sortStat,
            final boolean sorted
    ) {
        final MultiFTGSRequest.Builder builder = MultiFTGSRequest.newBuilder();

        final List<RemoteImhotepMultiSession> remoteSessions = new ArrayList<>();
        final Set<HostAndPort> allNodes = new HashSet<>();

        for (final SessionField sessionField : sessionsWithFields) {
            final RemoteImhotepMultiSession session = sessionField.session;
            remoteSessions.add(session);
            final String fieldName = sessionField.field;
            final List<HostAndPort> nodes = Arrays.stream(session.nodes).map(input ->
                    HostAndPort.newBuilder()
                            .setHost(input.getHostName())
                            .setPort(input.getPort())
                            .build()
            ).collect(Collectors.toList());
            allNodes.addAll(nodes);
            builder.addSessionInfoBuilder()
                    .setSessionId(session.getSessionId())
                    .addAllNodes(nodes)
                    .setField(fieldName);
        }

        final List<HostAndPort> allNodesList = Lists.newArrayList(allNodes);

        builder
                .addAllSelect(AggregateStatTree.allAsList(selects))
                .addAllFilter(AggregateStatTree.allAsList(filters))
                .setIsIntField(isIntField)
                .setTermLimit(termLimit)
                .setSortStat(sortStat)
                .setSortedFTGS(sorted)
                .addAllNodes(allNodesList);

        final Pair<Integer, HostAndPort>[] indexedServers = new Pair[allNodesList.size()];
        for (int i = 0; i < allNodesList.size(); i++) {
            HostAndPort hostAndPort = allNodesList.get(i);
            indexedServers[i] = new Pair<>(i, hostAndPort);
        }

        final MultiFTGSRequest baseRequest = builder.build();

        final FTGAIterator[] subIterators = new FTGAIterator[indexedServers.length];

        final Closer closer = Closer.create();
        closer.register(Closeables2.forArray(log, subIterators));
        // TODO: use a different executor maybe?
        try {
            remoteSessions.get(0).execute(subIterators, indexedServers, false, pair -> {
                final int index = pair.getFirst();
                final HostAndPort hostAndPort = pair.getSecond();
                // TODO: is setting up new ImhotepRemoteSessions the right thing to do?
                // TODO: needs to track FTGS bytes
                // Definitely don't close this session
                final ImhotepRemoteSession remoteSession = new ImhotepRemoteSession(hostAndPort.getHost(), hostAndPort.getPort(), "multi-session-id", null, ImhotepRemoteSession.DEFAULT_SOCKET_TIMEOUT);
                final MultiFTGSRequest proto = MultiFTGSRequest.newBuilder(baseRequest).setSplitIndex(index).build();
                return remoteSession.multiFTGS(proto);
            });
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }

        final FTGSModifiers modifiers = new FTGSModifiers(termLimit, sortStat, sorted);

        final FTGAIterator interleaver;
        if (sorted) {
            interleaver = new SortedFTGAInterleaver(subIterators);
        } else {
            interleaver = new UnsortedFTGAIterator(subIterators);
        }

        return modifiers.wrap(interleaver);
    }
}