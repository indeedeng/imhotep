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
import com.google.common.io.Closer;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.HasSessionId;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.longs.LongIterators;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
            interleaver = FTGSIteratorUtil.getTopTermsFTGSIterator(interleaver, params.termLimit, numStats, params.sortStat);
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
}
