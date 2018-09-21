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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.FTGSMerger;
import com.indeed.imhotep.FTGSSplitter;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupStatsDummyIterator;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MultiSessionMerger;
import com.indeed.imhotep.SortedFTGSInterleaver;
import com.indeed.imhotep.TermLimitedFTGSIterator;
import com.indeed.imhotep.UnsortedFTGSIterator;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSModifiers;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.metrics.aggregate.MultiFTGSIterator;
import com.indeed.imhotep.pool.GroupStatsPool;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.Either;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * @author jsgroth
 */
public class MTImhotepLocalMultiSession extends AbstractImhotepMultiSession<ImhotepLocalSession> {
    private static final Logger log = Logger.getLogger(MTImhotepLocalMultiSession.class);

    private final MemoryReservationContext memory;

    private final AtomicReference<Boolean> closed = new AtomicReference<>();

    private Either<Throwable,FTGSIterator>[] ftgsSplits;

    public MTImhotepLocalMultiSession(final String sessionId,
                                      final ImhotepLocalSession[] sessions,
                                      final MemoryReservationContext memory,
                                      final AtomicLong tempFileSizeBytesLeft,
                                      final String userName,
                                      final String clientName)
    {
        super(sessionId, sessions, tempFileSizeBytesLeft, userName, clientName);
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

        final GroupStatsPool pool = new GroupStatsPool(getNumGroups());

        executeRuntimeException(nullBuf, session -> {
            if (session.isFilteredOut()) {
                return null;
            }
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
    public FTGSIterator getFTGSIterator(final FTGSParams params) {
        if (sessions.length == 1) {
            try(final Closeable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                return persist(sessions[0].getFTGSIterator(params));
            } catch (final Exception e) {
                throw Throwables.propagate(e);
            }
        }

        final FTGSParams localSessionParams;
        // ftgs from subsession have to be sorted for merging.
        // and in case of top terms request we have to iterate through all terms
        if (params.isTopTerms()) {
            localSessionParams = params.sortedCopy().unlimitedCopy();
        } else {
            localSessionParams = params.sortedCopy();
        }

        return mergeFTGSIteratorsForSessions(sessions, params.termLimit, params.sortStat, params.sorted, session -> session.getFTGSIterator(localSessionParams));
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        if (sessions.length == 1) {
            try(final Closeable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                return persist(sessions[0].getSubsetFTGSIterator(intFields, stringFields));
            } catch (final Exception e) {
                throw Throwables.propagate(e);
            }
        }

        return mergeFTGSIteratorsForSessions(sessions, 0, -1, true, s -> s.getSubsetFTGSIterator(intFields, stringFields));
    }

    public synchronized FTGSIterator getFTGSIteratorSplit(final String[] intFields, final String[] stringFields, final int splitIndex, final int numSplits, final long termLimit) {
        checkSplitParams(splitIndex, numSplits);

        final FTGSIterator split = getSplitOrThrow(splitIndex, numSplits);
        if (split != null) {
            return split;
        }

        final FTGSSplitter[] ftgsIteratorSplitters = new FTGSSplitter[sessions.length];
        try {
            executeSessions(getSplitBufferThreads, ftgsIteratorSplitters, true, imhotepSession -> imhotepSession.getFTGSIteratorSplitter(intFields, stringFields, numSplits, termLimit));
            initSplits(ftgsIteratorSplitters, numSplits, termLimit);
        } catch (final Throwable t) {
            Closeables2.closeAll(log, ftgsIteratorSplitters);
            initSplitsWithError(t, numSplits);
            // not throwing here, exception will be created when trying to get split by index.
        }

        return getSplitOrThrow(splitIndex, numSplits);
    }

    public synchronized FTGSIterator getSubsetFTGSIteratorSplit(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final int splitIndex, final int numSplits) {
        checkSplitParams(splitIndex, numSplits);

        final FTGSIterator split = getSplitOrThrow(splitIndex, numSplits);
        if (split != null) {
            return split;
        }

        final FTGSSplitter[] ftgsIteratorSplitters = new FTGSSplitter[sessions.length];
        try {
            executeSessions(getSplitBufferThreads, ftgsIteratorSplitters, true, imhotepSession -> imhotepSession.getSubsetFTGSIteratorSplitter(intFields, stringFields, numSplits));
            initSplits(ftgsIteratorSplitters, numSplits, 0);
        } catch (final Throwable t) {
            Closeables2.closeAll(log, ftgsIteratorSplitters);
            initSplitsWithError(t, numSplits);
            // not throwing here, exception will be created when trying to get split by index.
        }

        return getSplitOrThrow(splitIndex, numSplits);
    }

    // Return cached FTGSIterator or throw cached error (and delete them from cache) or return null if there is no cached value.
    private FTGSIterator getSplitOrThrow(final int splitIndex, final int numSplits) {
        try {
            if ((ftgsSplits != null) && (ftgsSplits.length == numSplits) && (ftgsSplits[splitIndex] != null)) {
                final Either<Throwable, FTGSIterator> result = ftgsSplits[splitIndex];
                ftgsSplits[splitIndex] = null;
                return result.get();
            }
        } catch (final Throwable t) {
            throw Throwables.propagate(t);
        }

        return null;
    }

    private void initSplits(final FTGSSplitter[] splitters, final int numSplits, final long termLimit) {
        closeSplits();
        ftgsSplits = new Either[numSplits];
        for (int index = 0; index < numSplits; index++) {
            final int localIndex = index;
            final List<FTGSIterator> splits = Arrays.stream(splitters)
                    .map(splitter -> splitter.getFtgsIterators()[localIndex]).collect(Collectors.toList());

            if (sessions.length == 1) {
                ftgsSplits[index] = Either.Right.of(splits.get(0));
                continue;
            }
            FTGSIterator merger = new FTGSMerger(splits, null);
            if (termLimit > 0) {
                merger = new TermLimitedFTGSIterator(merger, termLimit);
            }
            ftgsSplits[index] = Either.Right.of(merger);
        }
    }

    private void initSplitsWithError(final Throwable error, final int numSplits) {
        closeSplits();
        ftgsSplits = new Either[numSplits];
        for (int index = 0; index < numSplits; index++) {
            ftgsSplits[index] = Either.Left.of(error);
        }
    }

    // Closing splits, but leaving errors.
    private void closeSplits() {
        if (ftgsSplits != null) {
            final int numSplits = ftgsSplits.length;
            for (int i = 0; i < numSplits; i++) {
                try {
                    final FTGSIterator ftgsSplit = ftgsSplits[i].get();
                    if (ftgsSplit != null) {
                        Closeables2.closeQuietly(ftgsSplit, log);
                    }
                    // Will execute this only if no error, exceptions will stay in ftgsSplit[]
                    ftgsSplits[i] = null;
                } catch (final Throwable ignored) {
                    // Closeables2.closeQuietly don't throw.
                    // This error is from Either.get(), ignoring it.
                }
            }
        }
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
            final FTGSParams params = new FTGSParams(intFields, strFields, 0, -1, false);
            final FTGSIterator iterator = getFTGSIterator(params);
            result = FTGSIteratorUtil.calculateDistinct(iterator);
        } finally {
            // return stats back.
            for (int i = 0; i < sessions.length; i++) {
                sessions[i].numStats = savedNumStats[i];
            }
        }
        return result;
    }

    public GroupStatsIterator mergeDistinctSplit(final String field,
                                                 final boolean isIntField,
                                                 final InetSocketAddress[] nodes,
                                                 final int splitIndex) {
        checkSplitParams(splitIndex, nodes.length);
        final String[] intFields = isIntField ? new String[]{field} : new String[0];
        final String[] stringFields = isIntField ? new String[0] : new String[]{field};
        final FTGSIterator iterator = mergeFTGSSplit(new FTGSParams(intFields, stringFields, 0, -1, false), nodes, splitIndex);
        return FTGSIteratorUtil.calculateDistinct(iterator);
    }

    @Override
    public void stringOrRegroup(final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, (ThrowingFunction<ImhotepSession, Object>) session -> {
            session.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public int regroup(final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepLocalSession, Integer>() {
            @Override
            public Integer apply(final ImhotepLocalSession session) throws ImhotepOutOfMemoryException {
                return session.regroup(fromGroups, toGroups, filterOutNotTargeted);
            }
        });

        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public void intOrRegroup(final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, (ThrowingFunction<ImhotepSession, Object>) session -> {
            session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, (ThrowingFunction<ImhotepSession, Integer>) session ->
                session.regroup(rawRules, errorOnCollisions));

        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    protected void postClose() {
        if (memory.usedMemory() > 0) {
            log.error("MTImhotepMultiSession [" + getSessionId() + "] is leaking! usedMemory = "+memory.usedMemory());
        }
        Closeables2.closeQuietly(memory, log);
        closeSplits();
        super.postClose();
    }

    private ImhotepRemoteSession createImhotepRemoteSession(final String sessionId, final InetSocketAddress address) {
        // TODO: Sharing tempFileSizeBytesLeft across sessions is not right ...
        return new ImhotepRemoteSession(address.getHostName(), address.getPort(),
                                        sessionId, tempFileSizeBytesLeft);
    }

    private ImhotepRemoteSession[] getRemoteSessions(final InetSocketAddress[] nodes) {
        return getRemoteSessions(getSessionId(), nodes);
    }

    private ImhotepRemoteSession[] getRemoteSessions(final String sessionId, final InetSocketAddress[] nodes) {
        final ImhotepRemoteSession[] remoteSessions = new ImhotepRemoteSession[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            remoteSessions[i] = createImhotepRemoteSession(sessionId, nodes[i]);
            remoteSessions[i].setNumStats(numStats);
            remoteSessions[i].addObserver(new RelayObserver());
        }
        return remoteSessions;
    }

    public FTGSIterator mergeFTGSSplit(final FTGSParams params, final InetSocketAddress[] nodes, final int splitIndex) {
        checkSplitParams(splitIndex, nodes.length);
        final long perSplitTermLimit = params.isTopTerms() ? 0 : params.termLimit;

        final ImhotepRemoteSession[] remoteSessions = getRemoteSessions(nodes);

        return mergeFTGSIteratorsForSessions(remoteSessions, params.termLimit, params.sortStat, params.sorted,
                s -> s.getFTGSIteratorSplit(params.intFields, params.stringFields, splitIndex, nodes.length, perSplitTermLimit));
    }

    public FTGSIterator mergeSubsetFTGSSplit(final Map<String, long[]> intFields,
                                             final Map<String, String[]> stringFields,
                                             final InetSocketAddress[] nodes,
                                             final int splitIndex) {
        checkSplitParams(splitIndex, nodes.length);
        final ImhotepRemoteSession[] remoteSessions = getRemoteSessions(nodes);
        return mergeFTGSIteratorsForSessions(remoteSessions, 0, -1, true,
                s -> s.getSubsetFTGSIteratorSplit(intFields, stringFields, splitIndex, nodes.length));
    }

    // splitIndex and numLocalSplits have NOTHING WHATSOEVER to do with eachother.
    // splitIndex refers to which NODE this is.
    // numLocalSplits refers to how many split/merge threads to use.
    // This is required as a parameter because it MUST MATCH across sessions but number of
    // cores is NOT guaranteed to be consistent over the runtime of a JVM.
    public FTGSIterator[] partialMergeFTGSSplit(final String remoteSessionId, final FTGSParams params, final InetSocketAddress[] nodes, final int splitIndex, final int numGlobalSplits, int numLocalSplits) {
        final FTGSIterator[] iterators = new FTGSIterator[nodes.length];
        final ImhotepRemoteSession[] remoteSessions = getRemoteSessions(remoteSessionId, nodes);

        final long perSplitTermLimit = params.isTopTerms() ? 0 : params.termLimit;

        if (numGlobalSplits == 1) {
            // In this case, we know that there is exactly one server involved -- this one! No need to request splits.
            // TODO: Probably no need to split it for merge threads either
            iterators[0] = remoteSessions[0].getFTGSIterator(params.intFields, params.stringFields, perSplitTermLimit);
        } else {
            checkSplitParams(splitIndex, numGlobalSplits);
            try {
                execute(mergeSplitBufferThreads, iterators, remoteSessions, false,
                        s -> s.getFTGSIteratorSplit(params.intFields, params.stringFields, splitIndex, numGlobalSplits, perSplitTermLimit));
            } catch (final Throwable t) {
                Closeables2.closeAll(log, iterators);
                throw Throwables.propagate(t);
            }
        }

        final Closer closer = Closer.create();
        try {
            return parallelDisjointSplitAndMerge(closer, iterators, numLocalSplits);
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }
    }

    // Element-wise zip the FTGSIterator's into a MultiSessionMerger,
    // Wrap with a MultiSessionWrapper, optionally wrap with topK or term limits,
    // and persist to a file. Return all of said files.
    public FTGAIterator[] zipElementWise(
            final FTGSModifiers modifiers,
            final List<FTGSIterator[]> subIteratorLists,
            final Function<MultiFTGSIterator, FTGAIterator> processor
    ) {
        final Closer closer = Closer.create();
        for (final FTGSIterator[] subIteratorList : subIteratorLists) {
            closer.register(Closeables2.forArray(log, subIteratorList));
        }
        try {
            Preconditions.checkArgument(subIteratorLists.size() > 0);
            final int numSplits = subIteratorLists.get(0).length;
            for (final FTGSIterator[] list : subIteratorLists) {
                Preconditions.checkArgument(list.length == numSplits);
            }
            // 1 list per SPLIT, where the elements are the elements that have the same index
            final FTGAIterator[] splitEntries = new FTGAIterator[numSplits];
            for (int i = 0; i < numSplits; i++) {
                final List<FTGSIterator> indexIterators = new ArrayList<>();
                for (final FTGSIterator[] list : subIteratorLists) {
                    indexIterators.add(list[i]);
                }
                splitEntries[i] = modifiers.wrap(processor.apply(new MultiSessionMerger(indexIterators, null)));
            }

            final FTGAIterator[] persisted = new FTGAIterator[numSplits];
            execute(persisted, splitEntries, true, this::persist);
            return persisted;
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }
    }

    private <T extends ImhotepSession> FTGSIterator mergeFTGSIteratorsForSessions(
            final T[] imhotepSessions,
            final long termLimit,
            final int sortStat,
            final boolean sorted,
            final ThrowingFunction<T, FTGSIterator> getIteratorFromSession
    ) {
        checkSplitParams(imhotepSessions.length);
        final FTGSIterator[] iterators = new FTGSIterator[imhotepSessions.length];

        try {
            execute(mergeSplitBufferThreads, iterators, imhotepSessions, false, getIteratorFromSession);
        } catch (final Throwable t) {
            Closeables2.closeAll(log, iterators);
            throw Throwables.propagate(t);
        }
        return parallelMergeFTGS(iterators, termLimit, sortStat, sorted);
    }

    private FTGSIterator parallelMergeFTGS(final FTGSIterator[] iterators,
                                           final long termLimit,
                                           final int sortStat,
                                           final boolean sorted) {
        final Closer closer = Closer.create();
        try {
            final FTGSIterator[] mergers = parallelDisjointSplitAndMerge(closer, iterators);
            final FTGSIterator[] persistedMergers = new FTGSIterator[mergers.length];
            closer.register(Closeables2.forArray(log, persistedMergers));
            execute(mergeSplitBufferThreads, persistedMergers, mergers, true, this::persist);

            final FTGSIterator interleaver = sorted ? new SortedFTGSInterleaver(persistedMergers) : new UnsortedFTGSIterator(persistedMergers);
            return new FTGSModifiers(termLimit, sortStat, sorted).wrap(interleaver);
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
    private FTGSIterator[] parallelDisjointSplitAndMerge(final Closer closer, final FTGSIterator[] iterators) {
        final int numSplits = Math.max(1, Runtime.getRuntime().availableProcessors()/2);
        return parallelDisjointSplitAndMerge(closer, iterators, numSplits);
    }

    private FTGSIterator[] parallelDisjointSplitAndMerge(final Closer closer, final FTGSIterator[] iterators, final int numSplits) {
        final FTGSIterator[][] iteratorSplits = new FTGSIterator[iterators.length][];

        try {
            execute(iteratorSplits, iterators, true, iterator -> FTGSSplitter.doSplit(
                    iterator,
                    numSplits,
                    981044833,
                    tempFileSizeBytesLeft
            ));
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
            mergers[j] = closer.register(new FTGSMerger(splits, null));
        }

        return mergers;
    }

    private FTGSIterator persist(final FTGSIterator iterator) throws IOException {
        return FTGSIteratorUtil.persist(log, getSessionId(), iterator);
    }

    private FTGAIterator persist(final FTGAIterator iterator) throws IOException {
        return FTGSIteratorUtil.persist(log, getSessionId(), iterator);
    }
}
