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
 package com.indeed.imhotep.service;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.Instrumentation.Keys;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.SortedFTGAInterleaver;
import com.indeed.imhotep.StrictCloser;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.UnsortedFTGAIterator;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSModifiers;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepServiceCore;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.BlockOutputStream;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.imhotep.metrics.aggregate.AggregateStatStack;
import com.indeed.imhotep.metrics.aggregate.ParseAggregate;
import com.indeed.imhotep.metrics.aggregate.ParseAggregate.SessionStatsInfo;
import com.indeed.imhotep.multisession.MultiSessionWrapper;
import com.indeed.imhotep.protobuf.AggregateStat;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.protobuf.MultiFTGSRequest;
import com.indeed.imhotep.protobuf.Operator;
import com.indeed.imhotep.protobuf.ShardBasicInfoMessage;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import com.indeed.imhotep.scheduling.ImhotepTask;
import com.indeed.imhotep.scheduling.SilentCloseable;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.WillClose;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author jplaisance
 */
public abstract class AbstractImhotepServiceCore
    implements ImhotepServiceCore, Instrumentation.Provider {


    private static final Logger log = Logger.getLogger(AbstractImhotepServiceCore.class);

    private final ExecutorService ftgsExecutor;
    private final ExecutorService multiFtgsExecutor;

    protected abstract SessionManager getSessionManager();

    private final Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    protected final class SessionObserver implements Instrumentation.Observer {
        private final String dataset;
        private final String sessionId;
        private final String username;
        private final String clientName;
        private final String ipAddress;

        SessionObserver(final String dataset,
                        final String sessionId,
                        final String username,
                        final String clientName,
                        final String ipAddress) {
            this.dataset   = dataset;
            this.sessionId = sessionId;
            this.username  = username;
            this.clientName = clientName;
            this.ipAddress = ipAddress;
        }

        public void onEvent(final Instrumentation.Event event) {
            event.getProperties().put(Keys.DATASET,    dataset);
            event.getProperties().put(Keys.SESSION_ID, sessionId);
            event.getProperties().put(Keys.USERNAME,   username);
            event.getProperties().put(Keys.CLIENT_NAME, clientName);
            event.getProperties().put(Keys.REMOTE_IP_ADDRESS, ipAddress);
            instrumentation.fire(event);
        }
    }

    protected AbstractImhotepServiceCore() {
        ftgsExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LocalImhotepServiceCore-FTGSWorker-%d").build());
        multiFtgsExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LocalImhotepServiceCore-MultiFTGSWorker-%d").build());
    }

    public void addObserver(final Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    public void removeObserver(final Instrumentation.Observer observer) {
        instrumentation.removeObserver(observer);
    }

    @Override
    public long handleGetTotalDocFreq(final String sessionId, final String[] intFields, final String[] stringFields) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, Long>) session -> session.getTotalDocFreq(intFields, stringFields));
    }

    @Override
    public GroupStatsIterator handleGetGroupStats(final String sessionId, final String groupsName, final List<String> stat) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, GroupStatsIterator, ImhotepOutOfMemoryException>) session -> session.getGroupStatsIterator(groupsName, stat));
    }

    @Override
    public GroupStatsIterator handleGetDistinct(final String sessionId, final String groupsName, final String field, final boolean isIntField) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, GroupStatsIterator>) session -> session.getDistinct(groupsName, field, isIntField));
    }

    @Override
    public GroupStatsIterator handleMergeDistinctSplit(final String sessionId,
                                                       final String groupsName,
                                                       final String field,
                                                       final boolean isIntField,
                                                       final HostAndPort[] nodes,
                                                       final int splitIndex) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, GroupStatsIterator>) session -> session.mergeDistinctSplit(groupsName, field, isIntField, nodes, splitIndex));
    }

    @Override
    public void handleGetFTGSIterator(final String sessionId, final String groupsName, final FTGSParams params, final OutputStream os) throws
            IOException, ImhotepOutOfMemoryException {
        doWithSession(sessionId, (IOOrOutOfMemoryFunction<MTImhotepLocalMultiSession, Void>) session -> {
            final FTGSIterator iterator = session.getFTGSIterator(groupsName, params);
            return sendFTGSIterator(iterator, os, false);
        });
    }

    @Override
    public void handleGetSubsetFTGSIterator(final String sessionId, final String groupsName, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats, final OutputStream os) throws IOException, ImhotepOutOfMemoryException {
        doWithSession(sessionId, (IOOrOutOfMemoryFunction<MTImhotepLocalMultiSession, Void>) session -> {
            final FTGSIterator iterator = session.getSubsetFTGSIterator(groupsName, intFields, stringFields, stats);
            return sendFTGSIterator(iterator, os, false);
        });
    }

    private Void sendFTGSIterator(@WillClose final FTGSIterator iterator, final OutputStream os, final boolean useFtgsPooledConnection) throws IOException {
        try (final SilentCloseable ignored = iterator) {
            final ImhotepResponse.Builder responseBuilder =
                    ImhotepResponse.newBuilder()
                            .setNumStats(iterator.getNumStats())
                            .setNumGroups(iterator.getNumGroups());
            log.debug("sending FTGS response");
            ImhotepProtobufShipping.sendProtobufNoFlush(responseBuilder.build(), os);
            if (useFtgsPooledConnection) {
                try (final BlockOutputStream blockOs = new BlockOutputStream(os)) {
                    writeFTGSIteratorToOutputStream(iterator, blockOs);
                }
            } else {
                writeFTGSIteratorToOutputStream(iterator, os);
            }
            os.flush();
            log.debug("FTGS response sent");
            return null;
        }
    }

    private void writeFTGSIteratorToOutputStream(final FTGSIterator iterator, final OutputStream os) throws IOException {
        final Future<?> future = ftgsExecutor.submit((Callable<Void>) () -> {
            try {
                // TODO: lock cpu, release on socket write by wrapping the socketstream and using a circular buffer,
                // or use nonblocking IO (NIO2) and only release when blocks
                FTGSIteratorUtil.writeFtgsIteratorToStream(iterator, os);
            } finally {
                Closeables2.closeQuietly(iterator, log);
            }
            return null;
        });
        try {
            // do a timed get so the task doesn't run infinitely
            future.get(30L, TimeUnit.MINUTES);
        } catch (final TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException(e);
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException)cause;
            }
            if (cause instanceof IOException) {
                throw (IOException)cause;
            }
            throw new RuntimeException(cause);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Void sendFTGAIterator(@WillClose final FTGAIterator iterator, final OutputStream os) throws IOException {
        try (final SilentCloseable ignored = iterator) {
            final ImhotepResponse.Builder responseBuilder =
                    ImhotepResponse.newBuilder()
                            .setNumStats(iterator.getNumStats())
                            .setNumGroups(iterator.getNumGroups());
            log.debug("sending FTGA response");
            ImhotepProtobufShipping.sendProtobufNoFlush(responseBuilder.build(), os);
            writeFTGAIteratorToOutputStream(iterator, os);
            os.flush();
            log.debug("FTGA response sent");
            return null;
        }
    }

    private Void writeFTGAIteratorToOutputStream(final FTGAIterator iterator, final OutputStream os) throws IOException {
        final Future<?> future = ftgsExecutor.submit((Callable<Void>) () -> {
            try {
                // TODO: lock cpu, release on socket write by wrapping the socketstream and using a circular buffer,
                // or use nonblocking IO (NIO2) and only release when blocks
                FTGSIteratorUtil.writeFtgaIteratorToStream(iterator, os);
            } finally {
                Closeables2.closeQuietly(iterator, log);
            }
            return null;
        });
        try {
            // do a timed get so the task doesn't run infinitely
            future.get(30L, TimeUnit.MINUTES);
        } catch (final TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException(e);
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException)cause;
            }
            if (cause instanceof IOException) {
                throw (IOException)cause;
            }
            throw new RuntimeException(cause);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public void handleGetFTGSIteratorSplit(
            final String sessionId,
            final String groupsName,
            final String[] intFields,
            final String[] stringFields,
            final OutputStream os,
            final int splitIndex,
            final int numSplits,
            final long termLimit,
            @Nullable final List<List<String>> stats,
            final boolean useFtgsPooledConnection) throws IOException, ImhotepOutOfMemoryException {
        doWithSession(sessionId, (IOOrOutOfMemoryFunction<MTImhotepLocalMultiSession, Void>) session -> {
            final FTGSIterator merger = session.getFTGSIteratorSplit(groupsName, intFields, stringFields, splitIndex, numSplits, termLimit, stats);
            return sendFTGSIterator(merger, os, useFtgsPooledConnection);
        });
    }

    @Override
    public void handleGetSubsetFTGSIteratorSplit(
            final String sessionId,
            final String groupsName,
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields,
            @Nullable final List<List<String>> stats,
            final OutputStream os,
            final int splitIndex,
            final int numSplits,
            final boolean useFtgsPooledConnection) throws IOException, ImhotepOutOfMemoryException {
        doWithSession(sessionId, (IOOrOutOfMemoryFunction<MTImhotepLocalMultiSession, Void>) session -> {
            final FTGSIterator merger = session.getSubsetFTGSIteratorSplit(groupsName, intFields, stringFields, stats, splitIndex, numSplits);
            return sendFTGSIterator(merger, os, useFtgsPooledConnection);
        });
    }

    @Override
    public void handleMergeFTGSIteratorSplit(final String sessionId,
                                             final String groupsName,
                                             final FTGSParams params,
                                             final OutputStream os,
                                             final HostAndPort[] nodes,
                                             final int splitIndex) throws IOException, ImhotepOutOfMemoryException {
        doWithSession(sessionId, (IOOrOutOfMemoryFunction<MTImhotepLocalMultiSession, Void>) session -> {
            final FTGSIterator merger = session.mergeFTGSSplit(groupsName, params, nodes, splitIndex);
            return sendFTGSIterator(merger, os, false);
        });
    }

    @Override
    public void handleMergeSubsetFTGSIteratorSplit(final String sessionId, final String groupsName, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats, final OutputStream os, final HostAndPort[] nodes, final int splitIndex) throws IOException, ImhotepOutOfMemoryException {
        doWithSession(sessionId, (IOOrOutOfMemoryFunction<MTImhotepLocalMultiSession, Void>) session -> {
            final FTGSIterator merger = session.mergeSubsetFTGSSplit(groupsName, intFields, stringFields, stats, nodes, splitIndex);
            return sendFTGSIterator(merger, os, false);
        });
    }

    // Processes the remote FTGS for each of the requested sessions, and splits it some number of ways, which
    // will be consistent for all elements in the list.
    // Each element in the returned list corresponds to a single input session.
    private List<FTGSIterator[]> getMultiSessionSplitIterators(
            final StrictCloser closer,
            final String validLocalSessionId,
            final HostAndPort[] nodes,
            final List<MultiFTGSRequest.MultiFTGSSession> sessionInfoList,
            final boolean isIntField,
            final int splitIndex,
            final long termLimit
    ) throws ImhotepOutOfMemoryException {
        final int numLocalSplits = Math.max(1, Runtime.getRuntime().availableProcessors()/2);
        final List<FTGSIterator[]> sessionSplitIterators = new ArrayList<>();

        final List<Future<FTGSIterator[]>> splitRemoteIteratorFutures = new ArrayList<>();

        for (final MultiFTGSRequest.MultiFTGSSession sessionInfo : sessionInfoList) {
            final List<List<String>> stats;
            if (sessionInfo.getHasStats()) {
                stats = sessionInfo.getStatsList()
                        .stream()
                        .map(docStat -> Lists.newArrayList(docStat.getStatList()))
                        .collect(Collectors.toList());
            } else {
                stats = null;
            }
            // This executor is fine because no real intensive work happens in these threads.
            final Future<FTGSIterator[]> future = multiFtgsExecutor.submit(new Callable<FTGSIterator[]>() {
                @Override
                public FTGSIterator[] call() throws Exception {
                    final HostAndPort[] sessionNodes = sessionInfo.getNodesList().toArray(new HostAndPort[0]);
                    final String localSessionChoice = sessionIsValid(sessionInfo.getSessionId()) ? sessionInfo.getSessionId() : validLocalSessionId;
                    final FTGSIterator[] sessionIterators = doWithSession(localSessionChoice, (IOOrOutOfMemoryFunction<MTImhotepLocalMultiSession, FTGSIterator[]>) session -> {
                        final String[] intFields = isIntField ? new String[]{sessionInfo.getField()} : new String[0];
                        final String[] stringFields = isIntField ? new String[0] : new String[]{sessionInfo.getField()};
                        final FTGSParams ftgsParams = new FTGSParams(intFields, stringFields, termLimit, -1, true, stats, StatsSortOrder.UNDEFINED);
                        final String groupsName = sessionInfo.getGroupsName();
                        return session.partialMergeFTGSSplit(sessionInfo.getSessionId(), groupsName, ftgsParams, sessionNodes, splitIndex, nodes.length, numLocalSplits);
                    });
                    closer.registerOrClose(Closeables2.forArray(log, sessionIterators));
                    if (numLocalSplits != sessionIterators.length) {
                        throw new IllegalStateException("Did not create the expected number of splits!");
                    }
                    return sessionIterators;
                }
            });
            splitRemoteIteratorFutures.add(future);
        }

        for (final Future<FTGSIterator[]> future : splitRemoteIteratorFutures) {
            try {
                sessionSplitIterators.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while retrieving and splitting remote FTGSes. System likely shutting down.");
            } catch (ExecutionException e) {
                Throwables.propagateIfInstanceOf(e.getCause(), ImhotepOutOfMemoryException.class);
                throw new RuntimeException("Error in retrieving and splitting remote FTGSes", e);
            }
        }
        return sessionSplitIterators;
    }

    @Override
    public void handleMergeMultiFTGSSplit(
            final MultiFTGSRequest request,
            final String validLocalSessionId,
            final OutputStream os,
            final HostAndPort[] nodes
    ) throws IOException, ImhotepOutOfMemoryException {
        final boolean isIntField = request.getIsIntField();
        final int splitIndex = request.getSplitIndex();
        final boolean sorted = request.getSortedFTGS();
        final StatsSortOrder statsSortOrder = request.getStatsSortOrder();

        final List<MultiFTGSRequest.MultiFTGSSession> sessionInfoList = request.getSessionInfoList();
        final FTGSModifiers modifiers = new FTGSModifiers(request.getTermLimit(), request.getSortStat(), request.getSortedFTGS(), statsSortOrder);

        final Map<String, SessionStatsInfo> sessionStatsInfos = new HashMap<>();
        for (int i = 0; i < sessionInfoList.size(); i++) {
            sessionStatsInfos.put(sessionInfoList.get(i).getSessionId(), new SessionStatsInfo(i));
        }

        final AggregateStatStack selects = new AggregateStatStack();
        for (final AggregateStat select : request.getSelectList()) {
            ParseAggregate.parse(sessionStatsInfos, selects, select);
        }

        final AggregateStatStack filters = new AggregateStatStack();
        for (final AggregateStat filter : request.getFilterList()) {
            ParseAggregate.parse(sessionStatsInfos, filters, filter);
        }

        final StrictCloser closer = new StrictCloser();
        try {
            final boolean useUpstreamTermLimit = (filters.size() == 0) && (request.getSortStat() == -1);
            final long upstreamTermLimit = useUpstreamTermLimit ? request.getTermLimit() : 0L;
            final List<FTGSIterator[]> sessionSplitIterators = getMultiSessionSplitIterators(closer, validLocalSessionId, nodes, sessionInfoList, isIntField, splitIndex, upstreamTermLimit);
            final FTGAIterator[] streams = doWithSession(validLocalSessionId, (Function<MTImhotepLocalMultiSession, FTGAIterator[]>) session -> {
                return session.zipElementWise(modifiers, sessionSplitIterators, x -> new MultiSessionWrapper(x, filters.toList(), selects.toList()));
            });
            closer.registerOrClose(Closeables2.forArray(log, streams));

            // If using term limits, then picking the lowest terms from amongst all inputs is required for ensuring that
            // we have the full data for the terms that are chosen.
            final FTGAIterator interleaver = closer.registerOrClose((sorted || useUpstreamTermLimit) ? new SortedFTGAInterleaver(streams) : new UnsortedFTGAIterator(streams));

            sendFTGAIterator(closer.registerOrClose(modifiers.wrap(interleaver)), os);
        } finally {
            Closeables2.closeQuietly(closer, log);
        }
    }

    @Override
    public GroupStatsIterator handleMergeMultiDistinctSplit(final MultiFTGSRequest request, final String validLocalSessionId, final HostAndPort[] nodes) throws ImhotepOutOfMemoryException {
        final boolean isIntField = request.getIsIntField();
        final int splitIndex = request.getSplitIndex();

        final List<MultiFTGSRequest.MultiFTGSSession> sessionInfoList = request.getSessionInfoList();

        final Map<String, SessionStatsInfo> sessionStatsInfos = new HashMap<>();
        for (int i = 0; i < sessionInfoList.size(); i++) {
            sessionStatsInfos.put(sessionInfoList.get(i).getSessionId(), new SessionStatsInfo(i));
        }

        final AggregateStatStack filters = new AggregateStatStack();
        for (final AggregateStat filter : request.getFilterList()) {
            ParseAggregate.parse(sessionStatsInfos, filters, filter);
        }


        // Support requests from before windowSize was a thing
        final List<Integer> windowSizes;
        if ((request.getWindowSizeCount() == 0) && (filters.size() > 0)) {
            windowSizes = Collections.nCopies(filters.size(), 1);
        } else {
            windowSizes = request.getWindowSizeList();
        }

        final ByteString protoParentGroups = request.getParentGroups();
        final int[] parentGroups;
        if (!protoParentGroups.isEmpty()) {
            parentGroups = ImhotepProtobufShipping.runLengthDecodeIntArray(protoParentGroups.toByteArray());
        } else {
            parentGroups = null;
        }

        final StrictCloser closeOnFailCloser = new StrictCloser();
        try {
            final List<FTGSIterator[]> sessionSplitIterators = getMultiSessionSplitIterators(closeOnFailCloser, validLocalSessionId, nodes, sessionInfoList, isIntField, splitIndex, 0);
            for (final FTGSIterator[] sessionSplitIterator : sessionSplitIterators) {
                closeOnFailCloser.registerOrClose(Closeables2.forArray(log, sessionSplitIterator));
            }
            return doWithSession(validLocalSessionId, (ThrowingFunction<MTImhotepLocalMultiSession, GroupStatsIterator, ImhotepOutOfMemoryException>) session -> {
                return session.mergeMultiDistinct(sessionSplitIterators, filters.toList(), windowSizes, parentGroups);
            });
        } catch (Throwable t) {
            Closeables2.closeQuietly(closeOnFailCloser, log);
            Throwables.propagateIfInstanceOf(t, ImhotepOutOfMemoryException.class);
            throw Throwables.propagate(t);
        }

    }

    @Override
    public abstract ImhotepStatusDump handleGetStatusDump(final boolean includeShardList);

    private static interface ThrowingFunction<A, B, T extends Throwable> {
        B apply(A a) throws T;
    }

    private interface IOOrOutOfMemoryFunction<A, B> {
        B apply(A a) throws IOException, ImhotepOutOfMemoryException;
    }

    private <Z, T extends Throwable> Z doWithSession(
            final String sessionId,
            final ThrowingFunction<MTImhotepLocalMultiSession, Z, T> f) throws T {
        final SharedReference<MTImhotepLocalMultiSession> sessionRef = getSessionManager().getSession(sessionId);
        try {
            final MTImhotepLocalMultiSession session = sessionRef.get();
            ImhotepTask.setup(session);
            return f.apply(session);
        } finally {
            Closeables2.closeQuietly(sessionRef, log);
            ImhotepTask.clear();
        }
    }

    private <Z> Z doWithSession(
            final String sessionId,
            final IOOrOutOfMemoryFunction<MTImhotepLocalMultiSession, Z> f) throws IOException, ImhotepOutOfMemoryException {
        final SharedReference<MTImhotepLocalMultiSession> sessionRef = getSessionManager().getSession(sessionId);
        try {
            final MTImhotepLocalMultiSession session = sessionRef.get();
            ImhotepTask.setup(session);
            return f.apply(session);
        } finally {
            Closeables2.closeQuietly(sessionRef, log);
            ImhotepTask.clear();
        }
    }

    private <Z> Z doWithSession(final String sessionId, final Function<MTImhotepLocalMultiSession, Z> f)  {
        final SharedReference<MTImhotepLocalMultiSession> sessionRef = getSessionManager().getSession(sessionId);
        try {
            final MTImhotepLocalMultiSession session = sessionRef.get();
            ImhotepTask.setup(session);
            return f.apply(session);
        } finally {
            Closeables2.closeQuietly(sessionRef, log);
            ImhotepTask.clear();
        }
    }

    @Override
    public int handleMultisplitRegroup(final String sessionId, final RegroupParams regroupParams, final GroupMultiRemapRule[] remapRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.regroup(regroupParams, remapRules, errorOnCollisions));
    }

    @Override
    public int handleMultisplitRegroup(final String sessionId, final RegroupParams regroupParams, final int numRemapRules, final Iterator<GroupMultiRemapRule> remapRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.regroup(regroupParams, numRemapRules, remapRules, errorOnCollisions));
    }

    @Override
    public int handleQueryRegroup(final String sessionId, final RegroupParams regroupParams, final QueryRemapRule remapRule) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.regroup(regroupParams, remapRule));
    }

    @Override
    public void handleIntOrRegroup(final String sessionId, final RegroupParams regroupParams, final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.intOrRegroup(regroupParams, field, terms, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public void handleStringOrRegroup(final String sessionId, final RegroupParams regroupParams, final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.stringOrRegroup(regroupParams, field, terms, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public void handleRandomRegroup(final String sessionId, final RegroupParams regroupParams, final String field, final boolean isIntField, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.randomRegroup(regroupParams, field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public void handleRandomMetricRegroup(final String sessionId,
                                          final RegroupParams regroupParams,
                                          final List<String> stat,
                                          final String salt,
                                          final double p,
                                          final int targetGroup,
                                          final int negativeGroup,
                                          final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.randomMetricRegroup(regroupParams, stat, salt, p, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public void handleRandomMetricMultiRegroup(final String sessionId,
                                               final RegroupParams regroupParams,
                                               final List<String> stat,
                                               final String salt,
                                               final int targetGroup,
                                               final double[] percentages,
                                               final int[] resultGroups) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.randomMetricMultiRegroup(regroupParams, stat, salt, targetGroup, percentages, resultGroups);
            return null;
        });
    }

    @Override
    public void handleRegexRegroup(final String sessionId, final RegroupParams regroupParams, final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.regexRegroup(regroupParams, field, regex, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public int handleMetricRegroup(final String sessionId, final RegroupParams regroupParams, final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.metricRegroup(regroupParams, stat, min, max, intervalSize, noGutters));
    }

    @Override
    public int handleRegroup(
            final String sessionId,
            final RegroupParams regroupParams,
            final int[] fromGroups,
            final int[] toGroups,
            final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>)
                session -> session.regroup(regroupParams, fromGroups, toGroups, filterOutNotTargeted));
    }

    public int handleMetricFilter(
            final String sessionId,
            final RegroupParams regroupParams,
            final List<String> stat,
            final long min,
            final long max,
            final boolean negate) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.metricFilter(regroupParams, stat, min, max, negate));
    }

    @Override
    public int handleMetricFilter(
            final String sessionId,
            final RegroupParams regroupParams,
            final List<String> stat,
            final long min,
            final long max,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.metricFilter(regroupParams, stat, min, max, targetGroup, negativeGroup, positiveGroup));
    }

    @Override
    public List<TermCount> handleApproximateTopTerms(final String sessionId, final String field, final boolean isIntField, final int k) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, List<TermCount>>) session -> session.approximateTopTerms(field, isIntField, k));
    }

    @Override
    public void handleConsolidateGroups(final String sessionId, final List<String> consolidatedGroupsList, final Operator operation, final String outputGroups) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, List<TermCount>, ImhotepOutOfMemoryException>) session -> {
            session.consolidateGroups(consolidatedGroupsList, operation, outputGroups);
            return null;
        });
    }

    @Override
    public void handleDeleteGroups(final String sessionId, final List<String> groupsToDelete) {
        doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, Void>) session -> {
            session.deleteGroups(groupsToDelete);
            return null;
        });
    }

    @Override
    public int handlePushStat(final String sessionId, final String metric) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.pushStat(metric));
    }

    @Override
    public int handlePopStat(final String sessionId) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, Integer>) AbstractImhotepMultiSession::popStat);
    }

    @Override
    public int handleGetNumGroups(final String sessionId, final String groupsName) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, Integer>) s -> s.getNumGroups(groupsName));
    }

    @Override
    public PerformanceStats handleGetPerformanceStats(final String sessionId, final boolean reset) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, PerformanceStats>) imhotepSession -> imhotepSession.getPerformanceStats(reset));
    }

    @Override
    public PerformanceStats handleCloseAndGetPerformanceStats(final String sessionId) {
        if(!getSessionManager().sessionIsValid(sessionId)) {
            return null;
        }
        final PerformanceStats stats = doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, PerformanceStats>) AbstractImhotepMultiSession::closeAndGetPerformanceStats);
        getSessionManager().removeAndCloseIfExists(sessionId);
        return stats;
    }

    @Override
    public void handleCreateDynamicMetric(final String sessionId, final String dynamicMetricName) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.createDynamicMetric(dynamicMetricName);
            return null;
        });
    }

    @Override
    public void handleUpdateDynamicMetric(final String sessionId, final String groupsName, final String dynamicMetricName, final int[] deltas) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.updateDynamicMetric(groupsName, dynamicMetricName, deltas);
            return null;
        });
    }

    @Override
    public void handleConditionalUpdateDynamicMetric(final String sessionId, final String dynamicMetricName, final RegroupCondition[] conditions, final int[] deltas) {
        doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, Void>) session -> {
            session.conditionalUpdateDynamicMetric(dynamicMetricName, conditions, deltas);
            return null;
        });
    }

    @Override
    public void handleGroupConditionalUpdateDynamicMetric(final String sessionId, final String groupsName, final String dynamicMetricName, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, Void>) session -> {
            session.groupConditionalUpdateDynamicMetric(groupsName, dynamicMetricName, groups, conditions, deltas);
            return null;
        });
    }

    public void handleGroupQueryUpdateDynamicMetric(final String sessionId, final String groupsName, final String dynamicMetricName, final int[] groups, final Query[] queries, final int[] deltas) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.groupQueryUpdateDynamicMetric(groupsName, dynamicMetricName, groups, queries, deltas);
            return null;
        });
    }

    @Override
    public void handleResetGroups(final String sessionId, final String groupsName) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.resetGroups(groupsName);
            return null;
        });
    }

    public abstract List<String> getShardsForSession(String sessionId);

    public abstract String handleOpenSession(
            String dataset,
            List<ShardBasicInfoMessage> shardRequestList,
            String username,
            String clientName,
            String ipAddress,
            byte priority,
            int clientVersion,
            int mergeThreadLimit,
            boolean optimizeGroupZeroLookups,
            String sessionId,
            AtomicLong tempFileSizeBytesLeft,
            final long sessionTimeout,
            final boolean useFtgsPooledConnection
    ) throws ImhotepOutOfMemoryException;

    @Override
    public boolean sessionIsValid(final String sessionId) {
        return getSessionManager().sessionIsValid(sessionId);
    }

    @Override
    public void handleCloseSession(final String sessionId) {
        getSessionManager().removeAndCloseIfExists(sessionId);
    }

    @Override
    public void handleCloseSession(final String sessionId, final Exception e) {
        getSessionManager().removeAndCloseIfExists(sessionId, e);
    }

    @Override
    public void close() {
        ftgsExecutor.shutdownNow();
        multiFtgsExecutor.shutdownNow();
    }

    @Override
    public <T> T handleBatchRequest(final String sessionId, final List<ImhotepCommand> firstCommands, final ImhotepCommand<T> lastCommand) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, T, ImhotepOutOfMemoryException>) session -> session.executeBatchRequest(firstCommands, lastCommand));
    }
}
