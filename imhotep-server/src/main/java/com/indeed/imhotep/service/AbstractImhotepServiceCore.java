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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.Instrumentation.Keys;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepServiceCore;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.scheduling.ImhotepTask;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
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

/**
 * @author jplaisance
 */
public abstract class AbstractImhotepServiceCore
    implements ImhotepServiceCore, Instrumentation.Provider {


    private static final Logger log = Logger.getLogger(AbstractImhotepServiceCore.class);

    private final ExecutorService ftgsExecutor;

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
    public GroupStatsIterator handleGetGroupStats(final String sessionId, final int stat) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, GroupStatsIterator>) session -> session.getGroupStatsIterator(stat));
    }

    @Override
    public GroupStatsIterator handleGetDistinct(final String sessionId, final String field, final boolean isIntField) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, GroupStatsIterator>) session -> session.getDistinct(field, isIntField));
    }

    @Override
    public GroupStatsIterator handleMergeDistinctSplit(final String sessionId,
                                                       final String field,
                                                       final boolean isIntField,
                                                       final InetSocketAddress[] nodes,
                                                       final int splitIndex) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, GroupStatsIterator>) session -> session.mergeDistinctSplit(field, isIntField, sessionId, nodes, splitIndex));
    }

    @Override
    public void handleGetFTGSIterator(final String sessionId, final String[] intFields, final String[] stringFields, final long termLimit, final int sortStat, final OutputStream os) throws
            IOException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, IOException>) session -> {
            final FTGSIterator merger = session.getFTGSIterator(intFields, stringFields, termLimit, sortStat);
            return sendFTGSIterator(merger, os);
        });
    }

    /**
     * Writes a success FTGS imhotep response protobuf to the provided stream.
     * Note: We can't send this until we know that the operation like GetFTGSIterator has succeeded
     * so it has to be sent here and not from ImhotepDaemon.
     * @param os output stream to write the successful response protobuf to.
     * @param numStats stats count in FTGS iterator
     * @param numGroups group count or greater value
     */
    private void sendSuccessFTGSResponse(final OutputStream os,
                                         final int numStats,
                                         final int numGroups) throws IOException {
        final ImhotepResponse.Builder responseBuilder =
                ImhotepResponse.newBuilder()
                        .setNumStats(numStats)
                        .setNumGroups(numGroups);
        ImhotepDaemon.sendResponse(responseBuilder.build(), os);
    }

    @Override
    public void handleGetSubsetFTGSIterator(final String sessionId, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final OutputStream os) throws IOException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, IOException>) session -> {
            final FTGSIterator merger = session.getSubsetFTGSIterator(intFields, stringFields);
            return sendFTGSIterator(merger, os);
        });
    }

    private Void sendFTGSIterator(final FTGSIterator merger, final OutputStream os) throws IOException {
        sendSuccessFTGSResponse(os, merger.getNumStats(), merger.getNumGroups());
        return writeFTGSIteratorToOutputStream(merger, os);
    }

    private Void writeFTGSIteratorToOutputStream(final FTGSIterator merger, final OutputStream os) throws IOException {
        final Future<?> future = ftgsExecutor.submit((Callable<Void>) () -> {
            try {
                // TODO: lock cpu, release on socket write by wrapping the socketstream and using a circular buffer,
                // or use nonblocking IO (NIO2) and only release when blocks
                FTGSOutputStreamWriter.write(merger, os);
            } finally {
                Closeables2.closeQuietly(merger, log);
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

    public void handleGetFTGSIteratorSplit(final String sessionId, final String[] intFields, final String[] stringFields, final OutputStream os, final int splitIndex, final int numSplits, final long termLimit) throws IOException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, IOException>) session -> {
            final FTGSIterator merger = session.getFTGSIteratorSplit(intFields, stringFields, splitIndex, numSplits, termLimit);
            return sendFTGSIterator(merger, os);
        });
    }

    @Override
    public void handleGetSubsetFTGSIteratorSplit(final String sessionId, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final OutputStream os, final int splitIndex, final int numSplits) throws IOException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, IOException>) session -> {
            final FTGSIterator merger = session.getSubsetFTGSIteratorSplit(intFields, stringFields, splitIndex, numSplits);
            return sendFTGSIterator(merger, os);
        });
    }

    @Override
    public void handleMergeFTGSIteratorSplit(final String sessionId,
                                             final String[] intFields,
                                             final String[] stringFields,
                                             final OutputStream os,
                                             final InetSocketAddress[] nodes,
                                             final int splitIndex,
                                             final long termLimit,
                                             final int sortStat) throws IOException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, IOException>) session -> {
            final FTGSIterator merger = session.mergeFTGSSplit(intFields, stringFields, sessionId, nodes, splitIndex, termLimit, sortStat);
            return sendFTGSIterator(merger, os);
        });
    }

    @Override
    public void handleMergeSubsetFTGSIteratorSplit(final String sessionId, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final OutputStream os, final InetSocketAddress[] nodes, final int splitIndex) throws IOException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, IOException>) session -> {
            final FTGSIterator merger = session.mergeSubsetFTGSSplit(intFields, stringFields, sessionId, nodes, splitIndex);
            return sendFTGSIterator(merger, os);
        });
    }

    @Override
    public abstract List<DatasetInfo> handleGetDatasetList();

    @Override
    public abstract List<ShardInfo> handleGetShardlistForTime(String dataset, long startUnixtime, long endUnixtime);

    @Override
    public abstract ImhotepStatusDump handleGetStatusDump(final boolean includeShardList);

    private static interface ThrowingFunction<A, B, T extends Throwable> {
        B apply(A a) throws T;
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
    public int handleRegroup(final String sessionId, final GroupRemapRule[] remapRules) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.regroup(remapRules));
    }

    public int handleRegroup(final String sessionId, final int numRemapRules, final Iterator<GroupRemapRule> remapRules) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.regroup2(numRemapRules, remapRules));
    }

    @Override
    public int handleMultisplitRegroup(final String sessionId, final GroupMultiRemapRule[] remapRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.regroup(remapRules, errorOnCollisions));
    }

    @Override
    public int handleMultisplitRegroup(final String sessionId, final int numRemapRules, final Iterator<GroupMultiRemapRule> remapRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.regroup(numRemapRules, remapRules, errorOnCollisions));
    }

    @Override
    public int handleQueryRegroup(final String sessionId, final QueryRemapRule remapRule) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.regroup(remapRule));
    }

    @Override
    public void handleIntOrRegroup(final String sessionId, final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public void handleStringOrRegroup(final String sessionId, final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public void handleRandomRegroup(final String sessionId, final String field, final boolean isIntField, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.randomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public void handleRandomMultiRegroup(final String sessionId, final String field, final boolean isIntField, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.randomMultiRegroup(field, isIntField, salt, targetGroup, percentages, resultGroups);
            return null;
        });
    }

    @Override
    public void handleRandomMetricRegroup(final String sessionId,
                                          final int stat,
                                          final String salt,
                                          final double p,
                                          final int targetGroup,
                                          final int negativeGroup,
                                          final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.randomMetricRegroup(stat, salt, p, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public void handleRandomMetricMultiRegroup(final String sessionId,
                                               final int stat,
                                               final String salt,
                                               final int targetGroup,
                                               final double[] percentages,
                                               final int[] resultGroups) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.randomMetricMultiRegroup(stat, salt, targetGroup, percentages, resultGroups);
            return null;
        });
    }

    @Override
    public void handleRegexRegroup(final String sessionId, final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public int handleMetricRegroup(final String sessionId, final int stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.metricRegroup(stat, min, max, intervalSize, noGutters));
    }

    @Override
    public int handleMetricRegroup2D(final String sessionId, final int xStat, final long xMin, final long xMax, final long xIntervalSize, final int yStat, final long yMin, final long yMax, final long yIntervalSize) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.metricRegroup2D(xStat, xMin, xMax, xIntervalSize, yStat, yMin, yMax, yIntervalSize));
    }

    public int handleMetricFilter(
            final String sessionId,
            final int stat,
            final long min,
            final long max,
            final boolean negate) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Integer, ImhotepOutOfMemoryException>) session -> session.metricFilter(stat, min, max, negate));
    }

    @Override
    public List<TermCount> handleApproximateTopTerms(final String sessionId, final String field, final boolean isIntField, final int k) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, List<TermCount>>) session -> session.approximateTopTerms(field, isIntField, k));
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
    public int handleGetNumGroups(final String sessionId) {
        return doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, Integer>) AbstractImhotepMultiSession::getNumGroups);
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
    public void handleUpdateDynamicMetric(final String sessionId, final String dynamicMetricName, final int[] deltas) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.updateDynamicMetric(dynamicMetricName, deltas);
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
    public void handleGroupConditionalUpdateDynamicMetric(final String sessionId, final String dynamicMetricName, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        doWithSession(sessionId, (Function<MTImhotepLocalMultiSession, Void>) session -> {
            session.groupConditionalUpdateDynamicMetric(dynamicMetricName, groups, conditions, deltas);
            return null;
        });
    }

    public void handleGroupQueryUpdateDynamicMetric(final String sessionId, final String dynamicMetricName, final int[] groups, final Query[] queries, final int[] deltas) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.groupQueryUpdateDynamicMetric(dynamicMetricName, groups, queries, deltas);
            return null;
        });
    }

    @Override
    public void handleRebuildAndFilterIndexes(final String sessionId, final String[] intFields, final String[] stringFields) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.rebuildAndFilterIndexes(Arrays.asList(intFields), Arrays.asList(stringFields));
            return null;
        });
    }

    @Override
    public void handleResetGroups(final String sessionId) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, (ThrowingFunction<MTImhotepLocalMultiSession, Void, ImhotepOutOfMemoryException>) session -> {
            session.resetGroups();
            return null;
        });
    }

    public abstract List<String> getShardIdsForSession(String sessionId);

    @Override
    public abstract String handleOpenSession(
            String dataset,
            List<String> shardRequestList,
            String username,
            String clientName,
            String ipAddress,
            int clientVersion,
            int mergeThreadLimit,
            boolean optimizeGroupZeroLookups,
            String sessionId,
            AtomicLong tempFileSizeBytesLeft,
            final long sessionTimeout
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
    }
}
