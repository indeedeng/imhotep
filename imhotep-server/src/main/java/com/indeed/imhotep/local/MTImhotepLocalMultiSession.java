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
package com.indeed.imhotep.local;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.FTGSMerger;
import com.indeed.imhotep.FTGSSplitter;
import com.indeed.imhotep.GroupStatsDummyIterator;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.SortedFTGSInterleaver;
import com.indeed.imhotep.TermLimitedFTGSIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.pool.GroupStatsPool;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


/**
 * @author jsgroth
 */
public class MTImhotepLocalMultiSession extends AbstractImhotepMultiSession<ImhotepLocalSession> {
    private static final Logger log = Logger.getLogger(MTImhotepLocalMultiSession.class);

    private final MemoryReservationContext memory;

    private final AtomicReference<Boolean> closed = new AtomicReference<>();

    protected FTGSSplitter[] ftgsIteratorSplitters;

    public MTImhotepLocalMultiSession(final ImhotepLocalSession[] sessions,
                                      final MemoryReservationContext memory,
                                      final AtomicLong tempFileSizeBytesLeft,
                                      final String userName,
                                      final String clientName)
    {
        super(sessions, tempFileSizeBytesLeft, userName, clientName);
        this.memory = memory;
        this.closed.set(false);
    }

    @Override
    protected void preClose() {
        if (closed.compareAndSet(false, true)) {
            super.preClose();
        }
    }

    @Override
    public long[] getGroupStats(final int stat) {

        final GroupStatsPool pool = new GroupStatsPool(numGroups);

        executeRuntimeException(nullBuf, session -> {
            final long[] buffer = pool.getBuffer();
            session.addGroupStats(stat, buffer);
            pool.returnBuffer(buffer);
            return null;
        });

        // TODO: run as a separate task maybe?
        return pool.getTotalResult();
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final int stat) {
        // there is two ways to create GroupStatsIterator in multisession:
        // create iterator over result of getGroupStats method or create merger for iterators.
        // In case of local multisession we are keeping full result in memory anyway,
        // so first option is better.
        return new GroupStatsDummyIterator(getGroupStats(stat));
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, final int sortStat) {
        final boolean topTermsByStat = sortStat >= 0;
        final long perSplitTermLimit = topTermsByStat ? 0 : termLimit;
        return mergeFTGSIteratorsForSessions(sessions, termLimit, sortStat, s -> s.getFTGSIterator(intFields, stringFields, perSplitTermLimit));
    }

    @Override
    public FTGSIterator[] getFTGSIteratorSplits(String[] intFields, String[] stringFields, long termLimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        return mergeFTGSIteratorsForSessions(sessions, -1, -1, s -> s.getSubsetFTGSIterator(intFields, stringFields));
    }

    @Override
    public FTGSIterator[] getSubsetFTGSIteratorSplits(
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized FTGSIterator getFTGSIteratorSplit(final String[] intFields, final String[] stringFields, final int splitIndex, final int numSplits, final long termLimit) {
        if (ftgsIteratorSplitters == null || (ftgsIteratorSplitters.length == 0 && sessions.length != 0) ||
                ftgsIteratorSplitters[0].isClosed()) {

            ftgsIteratorSplitters = new FTGSSplitter[sessions.length];
            try {
                executeSessions(getSplitBufferThreads, ftgsIteratorSplitters, false, imhotepSession -> imhotepSession.getFTGSIteratorSplitter(intFields, stringFields, numSplits, termLimit));
            } catch (final Throwable t) {
                Closeables2.closeAll(log, ftgsIteratorSplitters);
                throw Throwables.propagate(t);
            }
        }

        final List<FTGSIterator> splits = Arrays.stream(ftgsIteratorSplitters)
                .map(splitter -> splitter.getFtgsIterators()[splitIndex]).collect(Collectors.toList());

        if (sessions.length == 1) {
            return splits.get(0);
        }
        FTGSIterator merger = new FTGSMerger(splits, numStats, null);
        if(termLimit > 0) {
            merger = new TermLimitedFTGSIterator(merger, termLimit);
        }
        return merger;
    }

    @Override
    public synchronized FTGSIterator getSubsetFTGSIteratorSplit(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final int splitIndex, final int numSplits) {
        if (ftgsIteratorSplitters == null || (ftgsIteratorSplitters.length == 0 && sessions.length != 0) ||
                ftgsIteratorSplitters[0].isClosed()) {

            ftgsIteratorSplitters = new FTGSSplitter[sessions.length];
            try {
                executeSessions(getSplitBufferThreads, ftgsIteratorSplitters, false, imhotepSession -> imhotepSession.getSubsetFTGSIteratorSplitter(intFields, stringFields, numSplits));
            } catch (final Throwable t) {
                Closeables2.closeAll(log, ftgsIteratorSplitters);
                throw Throwables.propagate(t);
            }
        }
        final List<FTGSIterator> splits = Arrays.stream(ftgsIteratorSplitters)
                .map(splitter -> splitter.getFtgsIterators()[splitIndex]).collect(Collectors.toList());

        if (sessions.length == 1) {
            return splits.get(0);
        }
        return new FTGSMerger(splits, numStats, null);
    }

    @Override
    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        if (sessions.length == 1) {
            return sessions[0].getDistinct(field, isIntField);
        }
        // It's a hack.
        // We don't care about stats while calculating distinct.
        // And FTGS with no-stats is faster.
        // So we drop stats count, calculate distinct and return stats.
        final int[] savedNumStats = new int[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            savedNumStats[i] = sessions[i].numStats;
        }
        final GroupStatsIterator result;
        try {
            // drop stats.
            for (final ImhotepLocalSession session : sessions) {
                session.numStats = 0;
            }
            final String[] intFields = isIntField ? new String[]{field} : new String[0];
            final String[] strFields = isIntField ? new String[0] : new String[]{field};
            final FTGSIterator iterator = getFTGSIterator(intFields, strFields);
            result = FTGSIteratorUtil.calculateDistinct(iterator, getNumGroups());
        } finally {
            // return stats back.
            for (int i = 0; i < sessions.length; i++) {
                sessions[i].numStats = savedNumStats[i];
            }
        }
        return result;
    }

    @Override
    protected void postClose() {
        if (memory.usedMemory() > 0) {
            log.error("MTImhotepMultiSession is leaking! usedMemory = "+memory.usedMemory());
        }
        Closeables2.closeQuietly(memory, log);
        super.postClose();
    }

    private ImhotepRemoteSession createImhotepRemoteSession(final InetSocketAddress address,
                                                              final String sessionId,
                                                              final AtomicLong tempFileSizeBytesLeft) {
        return new ImhotepRemoteSession(address.getHostName(), address.getPort(),
                                        sessionId, tempFileSizeBytesLeft);
    }

    private ImhotepRemoteSession[] getRemoteSessions(final String sessionId, final InetSocketAddress[] nodes) {
        final ImhotepRemoteSession[] remoteSessions = new ImhotepRemoteSession[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            remoteSessions[i] = createImhotepRemoteSession(nodes[i], sessionId, tempFileSizeBytesLeft);
            remoteSessions[i].setNumStats(numStats);
            remoteSessions[i].addObserver(new RelayObserver());
        }
        return remoteSessions;
    }

    @Override
    public FTGSIterator mergeFTGSSplit(final String[] intFields, final String[] stringFields, final String sessionId, final InetSocketAddress[] nodes, final int splitIndex, final long termLimit, final int sortStat) {
        final boolean topTermsByStat = sortStat >= 0;
        final long perSplitTermLimit = topTermsByStat ? 0 : termLimit;

        final ImhotepRemoteSession[] remoteSessions = getRemoteSessions(sessionId, nodes);

        return mergeFTGSIteratorsForSessions(remoteSessions, termLimit, sortStat, s -> s.getFTGSIteratorSplit(intFields, stringFields, splitIndex, nodes.length, perSplitTermLimit));
    }

    @Override
    public FTGSIterator mergeSubsetFTGSSplit(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final String sessionId, final InetSocketAddress[] nodes, final int splitIndex) {
        final ImhotepRemoteSession[] remoteSessions = getRemoteSessions(sessionId, nodes);
        return mergeFTGSIteratorsForSessions(remoteSessions, 0, -1, (s) -> s.getSubsetFTGSIteratorSplit(intFields, stringFields, splitIndex, nodes.length));
    }

    private FTGSIterator mergeFTGSIteratorsForSessions(
            final ImhotepSession[] imhotepSessions,
            final long termLimit,
            final int sortStat,
            final ThrowingFunction<ImhotepSession, FTGSIterator> getIteratorFromSession
    ) {
        if (imhotepSessions.length == 1) {
            try {
                final FTGSIterator iterator = getIteratorFromSession.apply(imhotepSessions[0]);
                if ((sortStat >= 0) && (termLimit > 0)) {
                    try(final Closeable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                        return FTGSIteratorUtil.getTopTermsFTGSIterator(iterator, termLimit, numStats, sortStat);
                    }
                } else {
                    if (imhotepSessions[0] instanceof ImhotepRemoteSession) {
                        // If iterator is from remote session then it's already persisted
                        return iterator;
                    } else {
                        // If iterator is from local session then we have to persist it first
                        try(final Closeable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                            return persist(iterator);
                        }
                    }
                }
            } catch (final Exception e) {
                throw Throwables.propagate(e);
            }
        }
        final FTGSIterator[] iterators = new FTGSIterator[imhotepSessions.length];

        try {
            execute(mergeSplitBufferThreads, iterators, imhotepSessions, false, getIteratorFromSession);
        } catch (final Throwable t) {
            Closeables2.closeAll(log, iterators);
            throw Throwables.propagate(t);
        }
        return parallelMergeFTGS(iterators, termLimit, sortStat);
    }

    private FTGSIterator parallelMergeFTGS(final FTGSIterator[] iterators, final long termLimit, final int sortStat) {
        final Closer closer = Closer.create();
        try {
            final FTGSIterator[] mergers = parallelDisjointSplitAndMerge(closer, iterators);
            final FTGSIterator[] persistedMergers = new FTGSIterator[mergers.length];
            closer.register(Closeables2.forArray(log, persistedMergers));
            execute(mergeSplitBufferThreads, persistedMergers, mergers, true, this::persist);

            final FTGSIterator interleaver = new SortedFTGSInterleaver(persistedMergers);
            if (termLimit > 0) {
                if (sortStat >= 0) {
                    try(final Closeable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                        return FTGSIteratorUtil.getTopTermsFTGSIterator(interleaver, termLimit, numStats, sortStat);
                    }
                } else {
                    return new TermLimitedFTGSIterator(interleaver, termLimit);
                }
            } else {
                return interleaver;
            }
        } catch (final Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }
    }

    /**
     * shuffle the split FTGSIterators such that the workload of iteration is evenly split among the split processing threads
     * @param closer
     * @param iterators
     * @return the reshuffled FTGSIterator array
     * @throws IOException
     */
    private FTGSIterator[] parallelDisjointSplitAndMerge(final Closer closer, final FTGSIterator[] iterators) throws IOException {
        final FTGSIterator[][] iteratorSplits = new FTGSIterator[iterators.length][];

        final int numSplits = Math.max(1, Runtime.getRuntime().availableProcessors()/2);
        try {
            execute(iteratorSplits, iterators, true, iterator -> new FTGSSplitter(
                    iterator,
                    numSplits,
                    numStats,
                    981044833
            ).getFtgsIterators());
        } catch (final ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            for (final FTGSIterator[] splits : iteratorSplits) {
                if (splits != null) {
                    for (final FTGSIterator split : splits) {
                        closer.register(split);
                    }
                }
            }
        }

        final FTGSIterator[] mergers = new FTGSIterator[numSplits];
        for (int j = 0; j < numSplits; j++) {
            final List<FTGSIterator> splits = Lists.newArrayList();
            for (int i = 0; i < iterators.length; i++) {
                splits.add(iteratorSplits[i][j]);
            }
            mergers[j] = closer.register(new FTGSMerger(splits, numStats, null));
        }

        return mergers;
    }

    private FTGSIterator persist(final FTGSIterator iterator) throws IOException {
        return FTGSIteratorUtil.persist(log, iterator, numStats);
    }
}
