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
import com.indeed.imhotep.GroupStatsIteratorCombiner;
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
import com.indeed.imhotep.metrics.aggregate.AggregateStat;
import com.indeed.imhotep.metrics.aggregate.MultiFTGSIterator;
import com.indeed.imhotep.pool.GroupStatsPool;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.Either;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.annotation.WillNotClose;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
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

        return mergeFTGSIteratorsForSessions(sessions, params.termLimit, params.sortStat, params.sorted, params.statsSortOrder, session -> session.getFTGSIterator(localSessionParams));
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

        return mergeFTGSIteratorsForSessions(sessions, 0, -1, true, StatsSortOrder.UNDEFINED, s -> s.getSubsetFTGSIterator(intFields, stringFields));
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
            final FTGSParams params = new FTGSParams(intFields, strFields, 0, -1, false, StatsSortOrder.UNDEFINED);
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
                                                 final HostAndPort[] nodes,
                                                 final int splitIndex) {
        checkSplitParams(splitIndex, nodes.length);
        final String[] intFields = isIntField ? new String[]{field} : new String[0];
        final String[] stringFields = isIntField ? new String[0] : new String[]{field};
        final FTGSIterator iterator = mergeFTGSSplit(new FTGSParams(intFields, stringFields, 0, -1, false, StatsSortOrder.UNDEFINED), nodes, splitIndex);
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
        return new ImhotepRemoteSession(address.getHostName(), address.getPort(), sessionId, tempFileSizeBytesLeft);
    }

    private ImhotepRemoteSession[] getRemoteSessions(final InetSocketAddress[] nodes) {
        return getRemoteSessions(getSessionId(), nodes);
    }

    private ImhotepRemoteSession getRemoteSession(final String sessionId, final HostAndPort hostAndPort) {
        return getRemoteSession(sessionId, new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort()));
    }

    private ImhotepRemoteSession getRemoteSession(final String sessionId, final InetSocketAddress node) {
        final ImhotepRemoteSession session = createImhotepRemoteSession(sessionId, node);
        session.setNumStats(numStats);
        session.addObserver(new RelayObserver());
        return session;
    }

    private ImhotepRemoteSession[] getRemoteSessions(final String sessionId, final InetSocketAddress[] nodes) {
        final ImhotepRemoteSession[] remoteSessions = new ImhotepRemoteSession[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            remoteSessions[i] = getRemoteSession(sessionId, nodes[i]);
        }
        return remoteSessions;
    }

    public FTGSIterator mergeFTGSSplit(final FTGSParams params, final HostAndPort[] nodes, final int splitIndex) {
        checkSplitParams(splitIndex, nodes.length);
        final long perSplitTermLimit = params.isTopTerms() ? 0 : params.termLimit;
        final String sessionId = getSessionId();

        return mergeFTGSIteratorsForSessions(nodes, params.termLimit, params.sortStat, params.sorted, params.statsSortOrder,
                node -> getRemoteSession(sessionId, node).getFTGSIteratorSplit(params.intFields, params.stringFields, splitIndex, nodes.length, perSplitTermLimit));
    }

    public FTGSIterator mergeSubsetFTGSSplit(final Map<String, long[]> intFields,
                                             final Map<String, String[]> stringFields,
                                             final HostAndPort[] nodes,
                                             final int splitIndex) {
        checkSplitParams(splitIndex, nodes.length);
        final String sessionId = getSessionId();
        return mergeFTGSIteratorsForSessions(nodes, 0, -1, true, StatsSortOrder.UNDEFINED,
                node -> getRemoteSession(sessionId, node).getSubsetFTGSIteratorSplit(intFields, stringFields, splitIndex, nodes.length));
    }

    // splitIndex and numLocalSplits have NOTHING WHATSOEVER to do with eachother.
    // splitIndex refers to which NODE this is.
    // numLocalSplits refers to how many split/merge threads to use.
    // This is required as a parameter because it MUST MATCH across sessions but number of
    // cores is NOT guaranteed to be consistent over the runtime of a JVM.
    public FTGSIterator[] partialMergeFTGSSplit(final String remoteSessionId, final FTGSParams params, final HostAndPort[] nodes, final int splitIndex, final int numGlobalSplits, int numLocalSplits) {
        final FTGSIterator[] iterators = new FTGSIterator[nodes.length];

        final long perSplitTermLimit = params.isTopTerms() ? 0 : params.termLimit;

        Preconditions.checkState(nodes.length <= numGlobalSplits, "Number of nodes must be <= numGlobalSplits");

        if (numGlobalSplits == 1) {
            // In this case, we know that there is exactly one server involved -- this one! No need to request splits.
            Preconditions.checkState(splitIndex == 0, "Split index must be zero if there's only 1 split!");
            // This session exists solely to make remote calls and should never be closed.
            // Closing it would close the session, and make future operations fail.
            // This is similar to mergeFTGSSplit.
            final ImhotepRemoteSession remoteSession = getRemoteSession(remoteSessionId, nodes[0]);
            iterators[0] = remoteSession.getFTGSIterator(params.intFields, params.stringFields, perSplitTermLimit);
        } else {
            checkSplitParams(splitIndex, numGlobalSplits);
            try {
                execute(mergeSplitBufferThreads, iterators, nodes, false,
                        // This session exists solely to make remote calls and should never be closed.
                        // Closing it would close the session, and make future operations fail.
                        // This is similar to mergeFTGSSplit.
                        node -> getRemoteSession(remoteSessionId, node).getFTGSIteratorSplit(params.intFields, params.stringFields, splitIndex, numGlobalSplits, perSplitTermLimit, true));
            } catch (final Throwable t) {
                Closeables2.closeAll(log, iterators);
                throw Throwables.propagate(t);
            }
        }

        final Closer closeOnFailCloser = Closer.create();
        try {
            return parallelDisjointSplitAndMerge(closeOnFailCloser, iterators, numLocalSplits);
        } catch (Throwable t) {
            Closeables2.closeQuietly(closeOnFailCloser, log);
            throw Throwables.propagate(t);
        }
    }

    // Element-wise zip the FTGSIterator's into a MultiSessionMerger,
    // Wrap with a MultiSessionWrapper, optionally wrap with topK or term limits,
    // and persist to a file. Return all of said files.
    public FTGAIterator[] zipElementWise(
            final FTGSModifiers modifiers,
            @WillClose final List<FTGSIterator[]> subIteratorLists,
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
                final MultiSessionMerger sessionMerger = closer.register(new MultiSessionMerger(indexIterators, null));
                final FTGAIterator processed = closer.register(processor.apply(sessionMerger));
                splitEntries[i] = closer.register(modifiers.wrap(processed));
            }
            final FTGAIterator[] persisted = new FTGAIterator[numSplits];
            execute(persisted, splitEntries, true, this::persist);
            return persisted;
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        } finally {
            Closeables2.closeQuietly(closer, log);
        }
    }

    // Element-wise zip the FTGSIterator's into a MultiSessionMerger,
    // count the number of terms that pass each filter for each group on each merger,
    // and return a GroupStatsIterator that adds those counts together.
    public GroupStatsIterator mergeMultiDistinct(
            @WillClose final List<FTGSIterator[]> subIteratorLists,
            final List<AggregateStat> filters,
            final List<Integer> windowSizes,
            @Nullable final int[] parentGroups) {
        final Closer closer = Closer.create();
        final Closer closeOnFailCloser = Closer.create();
        try {
            for (final FTGSIterator[] subIteratorList : subIteratorLists) {
                closer.register(Closeables2.forArray(log, subIteratorList));
            }
            Preconditions.checkArgument(subIteratorLists.size() > 0);
            final int numSplits = subIteratorLists.get(0).length;
            for (final FTGSIterator[] list : subIteratorLists) {
                Preconditions.checkArgument(list.length == numSplits);
            }
            // 1 list per SPLIT, where the elements are the elements that have the same index
            final MultiFTGSIterator[] splitEntries = new MultiFTGSIterator[numSplits];
            for (int i = 0; i < numSplits; i++) {
                final List<FTGSIterator> indexIterators = new ArrayList<>();
                for (final FTGSIterator[] list : subIteratorLists) {
                    indexIterators.add(list[i]);
                }
                splitEntries[i] = closer.register(new MultiSessionMerger(indexIterators, null));
            }

            final GroupStatsIterator[] threadCounts = new GroupStatsIterator[numSplits];
            closeOnFailCloser.register(Closeables2.forArray(log, threadCounts));
            execute(threadCounts, splitEntries, true, x -> calculateMultiDistinct(x, filters, windowSizes, parentGroups));
            return new GroupStatsIteratorCombiner(threadCounts);
        } catch (ExecutionException e) {
            Closeables2.closeQuietly(closeOnFailCloser, log);
            throw Throwables.propagate(e);
        } finally {
            Closeables2.closeQuietly(closer, log);
        }
    }

    /**
     *
     * @param iterator input iterator
     * @param filters one filter per HAVING
     * @return group stats iterator with num stats == filters.size(), corresponding element-wise
     */
    public static GroupStatsIterator calculateMultiDistinctSimple(@WillNotClose final MultiFTGSIterator iterator, final List<AggregateStat> filters) {
        Preconditions.checkState(iterator.nextField(), "MultiFTGSIterator had no fields, expected exactly 1");
        Preconditions.checkState("magic".equals(iterator.fieldName()), "MultiFTGSIterator has field name \"%s\", expected \"magic\"", iterator.fieldName());

        final int numFilters = filters.size();
        // Layout is group0 f0, group0 f1, group 0 f2, group1 f0, group1 f1, group1 f2, ...
        final long[] data = new long[iterator.getNumGroups() * numFilters];
        while (iterator.nextTerm()) {
            while (iterator.nextGroup()) {
                final int group = iterator.group();
                for (int i = 0; i < numFilters; i++) {
                    final AggregateStat filter = filters.get(i);
                    if (AggregateStat.truthy(filter.apply(iterator))) {
                        data[(group * numFilters) + i] += 1;
                    }
                }
            }
        }

        Preconditions.checkState(!iterator.nextField(), "MultiFTGSIterator had more than 1 field, expected exactly 1");

        return new GroupStatsDummyIterator(data);
    }

    private static class DistinctWindowFilterInfo {
        private final int windowSize;
        private final AggregateStat filter;
        private final int originalIndex;
        private final long[] groupCounts;

        private DistinctWindowFilterInfo(final int windowSize, final AggregateStat filter, final int originalIndex, final int numGroups) {
            this.windowSize = windowSize;
            this.filter = filter;
            this.originalIndex = originalIndex;
            this.groupCounts = new long[numGroups];
        }
    }

    public GroupStatsIterator calculateMultiDistinct(
            @WillNotClose final MultiFTGSIterator iterator,
            final List<AggregateStat> filters,
            final List<Integer> windowSizes,
            @Nullable final int[] parentGroups) throws ImhotepOutOfMemoryException {
        Preconditions.checkArgument(filters.size() == windowSizes.size(), "filters.size() must match windowSizes.size()");

        if (windowSizes.stream().allMatch(x -> x == 1)) {
            return calculateMultiDistinctSimple(iterator, filters);
        }

        Preconditions.checkNotNull(parentGroups, "Parent groups must be non-null for windowed multi distinct");

        final int numFilters = filters.size();
        // Can't use the iterator max group because we need to share the global view of numGroups to
        // make the windows fully cover what they need to.
        // We receive the global view from the parentGroups parameter passed to us.
        final int numGroups = parentGroups.length;

        final List<DistinctWindowFilterInfo> filterInfos = new ArrayList<>();
        for (int i = 0; i < numFilters; i++) {
            filterInfos.add(new DistinctWindowFilterInfo(windowSizes.get(i), filters.get(i), i, numGroups));
        }

        final List<List<DistinctWindowFilterInfo>> groupedByWindowSize = Lists.newArrayList(
                filterInfos.stream()
                .collect(Collectors.groupingBy(x -> x.windowSize, Collectors.toList()))
                .values());

        final int[] numStats = iterator.numStats();
        final int numIterators = numStats.length;
        final int totalNumStats = Arrays.stream(numStats).sum();

        final long claimedMemory = totalNumStats * numGroups * Long.BYTES;
        if (!memory.claimMemory(claimedMemory)) {
            throw newImhotepOutOfMemoryException();
        }
        try {
            // g = group
            // s = stat
            // Layout: { g0s0, g0s1, g1s0, g1s1, ... }
            final long[] groupStatValues = new long[totalNumStats * numGroups];
            final BitSet groupsSeen = new BitSet(numGroups);

            final int[] iteratorOffsets = new int[numStats.length];
            for (int i = 1; i < iteratorOffsets.length; i++) {
                iteratorOffsets[i] = iteratorOffsets[i - 1] + numStats[i];
            }

            final long[] tmpRollingSumsBuffer = new long[totalNumStats];

            //noinspection IOResourceOpenedButNotSafelyClosed
            final FrozenGroupStatsMultiFTGSIterator frozenGroupStatsMultiFTGSIterator = new FrozenGroupStatsMultiFTGSIterator(tmpRollingSumsBuffer, iteratorOffsets, iterator);

            Preconditions.checkState(iterator.nextField(), "MultiFTGSIterator had no fields, expected exactly 1");
            Preconditions.checkState("magic".equals(iterator.fieldName()), "MultiFTGSIterator has field name \"%s\", expected \"magic\"", iterator.fieldName());

            while (iterator.nextTerm()) {
                // Collect all of the stats for all of the groups
                while (iterator.nextGroup()) {
                    final int group = iterator.group();
                    groupsSeen.set(group);
                    final int groupOffset = group * totalNumStats;
                    for (int i = 0; i < numIterators; i++) {
                        iterator.groupStats(i, groupStatValues, groupOffset + iteratorOffsets[i]);
                    }
                }

                for (final List<DistinctWindowFilterInfo> singleWindowSize : groupedByWindowSize) {
                    final int windowSize = singleWindowSize.get(0).windowSize;

                    int maxGroupExcl = 0;
                    int group = 0;

                    while (true) {
                        group = groupsSeen.nextSetBit(group);
                        if (group == -1) {
                            break;
                        }
                        frozenGroupStatsMultiFTGSIterator.setGroup(group);

                        final int startGroup = group;
                        final int parentGroup = parentGroups[group];
                        // No need to look backwards because any values that weren't in the bitset also won't have stats set.
                        System.arraycopy(groupStatValues, group * totalNumStats, tmpRollingSumsBuffer, 0, totalNumStats);

                        // Process all values at this group and moving forward until we hit the end of the parent group
                        // or a place not covered by the union of the windows.
                        while (true) {
                            if (groupsSeen.get(group)) {
                                maxGroupExcl = group + windowSize;
                            }

                            for (final DistinctWindowFilterInfo filterInfo : singleWindowSize) {
                                if (AggregateStat.truthy(filterInfo.filter.apply(frozenGroupStatsMultiFTGSIterator))) {
                                    filterInfo.groupCounts[group] += 1;
                                }
                            }

                            group += 1;
                            frozenGroupStatsMultiFTGSIterator.setGroup(group);

                            if ((group >= numGroups) || (group >= maxGroupExcl) || (parentGroups[group] != parentGroup)) {
                                break;
                            } else {
                                final int subtractedGroup = group - windowSize;
                                // Subtract out `group - windowSize` if necessary
                                if (subtractedGroup >= startGroup) {
                                    for (int stat = 0; stat < totalNumStats; stat++) {
                                        tmpRollingSumsBuffer[stat] -= groupStatValues[(subtractedGroup * totalNumStats) + stat];
                                    }
                                }

                                // Add in new `group`
                                for (int stat = 0; stat < totalNumStats; stat++) {
                                    tmpRollingSumsBuffer[stat] += groupStatValues[(group * totalNumStats) + stat];
                                }
                            }
                        }
                    }
                }

                Arrays.fill(groupStatValues, 0L);
                groupsSeen.clear();
            }
        } finally {
            memory.releaseMemory(claimedMemory);
        }

        Preconditions.checkState(!iterator.nextField(), "MultiFTGSIterator had more than 1 field, expected exactly 1");

        // Layout is group0 f0, group0 f1, group 0 f2, group1 f0, group1 f1, group1 f2, ...
        final long[] data = new long[numGroups * numFilters];

        for (final DistinctWindowFilterInfo filterInfo : filterInfos) {
            for (int group = 0; group < numGroups; group++) {
                data[(group * numFilters) + filterInfo.originalIndex] = filterInfo.groupCounts[group];
            }
        }

        return new GroupStatsDummyIterator(data);
    }

    private <T> FTGSIterator mergeFTGSIteratorsForSessions(
            final T[] imhotepSessions,
            final long termLimit,
            final int sortStat,
            final boolean sorted,
            final StatsSortOrder statsSortOrder,
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
        return parallelMergeFTGS(iterators, termLimit, sortStat, sorted, statsSortOrder);
    }

    private FTGSIterator parallelMergeFTGS(@WillClose final FTGSIterator[] iterators,
                                           final long termLimit,
                                           final int sortStat,
                                           final boolean sorted,
                                           final StatsSortOrder statsSortOrder) {
        final Closer closer = Closer.create();
        try {
            final FTGSIterator[] mergers = parallelDisjointSplitAndMerge(closer, iterators);
            final FTGSIterator[] persistedMergers = new FTGSIterator[mergers.length];
            closer.register(Closeables2.forArray(log, persistedMergers));
            execute(mergeSplitBufferThreads, persistedMergers, mergers, true, this::persist);

            final FTGSIterator interleaver = sorted ? new SortedFTGSInterleaver(persistedMergers) : new UnsortedFTGSIterator(persistedMergers);
            return new FTGSModifiers(termLimit, sortStat, sorted, statsSortOrder).wrap(interleaver);
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
    private FTGSIterator[] parallelDisjointSplitAndMerge(final Closer closer, @WillClose final FTGSIterator[] iterators) {
        final int numSplits = Math.max(1, Runtime.getRuntime().availableProcessors()/2);
        return parallelDisjointSplitAndMerge(closer, iterators, numSplits);
    }

    private FTGSIterator[] parallelDisjointSplitAndMerge(final Closer closer, @WillClose final FTGSIterator[] iterators, final int numSplits) {
        final FTGSIterator[][] iteratorSplits = new FTGSIterator[iterators.length][];

        try {
            execute(iteratorSplits, iterators, true, iterator -> FTGSSplitter.doSplit(
                    iterator,
                    numSplits,
                    981044833,
                    tempFileSizeBytesLeft
            ));
        } catch (final ExecutionException e) {
            throw newRuntimeException(e);
        } finally {
            Closeables2.closeAll(log, iterators);
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

    private FTGSIterator persist(@WillClose final FTGSIterator iterator) throws IOException {
        return FTGSIteratorUtil.persist(log, getSessionId(), iterator);
    }

    private FTGAIterator persist(@WillClose final FTGAIterator iterator) throws IOException {
        return FTGSIteratorUtil.persist(log, getSessionId(), iterator);
    }
}
