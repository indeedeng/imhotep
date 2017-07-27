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
 package com.indeed.imhotep.service;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.indeed.flamdex.query.Query;
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
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepServiceCore;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
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

/**
 * @author jplaisance
 */
public abstract class AbstractImhotepServiceCore
    implements ImhotepServiceCore, Instrumentation.Provider {


    private static final Logger log = Logger.getLogger(AbstractImhotepServiceCore.class);

    private final ExecutorService ftgsExecutor;

    protected abstract SessionManager getSessionManager();

    protected final Instrumentation.ProviderSupport instrumentation =
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
        return doWithSession(sessionId, new Function<ImhotepSession, Long>() {
            public Long apply(final ImhotepSession session) {
                return session.getTotalDocFreq(intFields, stringFields);
            }
        });
    }

    @Override
    public long[] handleGetGroupStats(final String sessionId, final int stat) {
        return doWithSession(sessionId, new Function<ImhotepSession, long[]>() {
            public long[] apply(final ImhotepSession session) {
                return session.getGroupStats(stat);
            }
        });
    }

    @Override
    public void handleGetFTGSIterator(final String sessionId, final String[] intFields, final String[] stringFields, final long termLimit, final int sortStat, final OutputStream os) throws
            IOException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, IOException>() {
            public Void apply(final ImhotepSession session) throws IOException {
                final int numStats = getSessionManager().getNumStats(sessionId);
                final FTGSIterator merger = session.getFTGSIterator(intFields, stringFields, termLimit, sortStat);
                sendSuccessResponse(os);
                return writeFTGSIteratorToOutputStream(numStats, merger, os);
            }
        });
    }

    /**
     * Writes a success imhotep response protobuf to the provided stream.
     * Note: We can't send this until we know that the operation like GetFTGSIterator has succeeded
     * so it has to be sent here and not from ImhotepDaemon.
     * @param os output stream to write the successful response protobuf to.
     */
    private void sendSuccessResponse(final OutputStream os) throws IOException {
        final ImhotepResponse.Builder responseBuilder = ImhotepResponse.newBuilder();
        ImhotepDaemon.sendResponse(responseBuilder.build(), os);
    }

    @Override
    public void handleGetSubsetFTGSIterator(final String sessionId, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final OutputStream os) throws IOException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, IOException>() {
            public Void apply(final ImhotepSession session) throws IOException {
                final int numStats = getSessionManager().getNumStats(sessionId);
                final FTGSIterator merger = session.getSubsetFTGSIterator(intFields, stringFields);
                sendSuccessResponse(os);
                return writeFTGSIteratorToOutputStream(numStats, merger, os);
            }
        });
    }

    private Void writeFTGSIteratorToOutputStream(final int numStats, final FTGSIterator merger, final OutputStream os) throws IOException {
        final Future<?> future = ftgsExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws IOException {
                try {
                    FTGSOutputStreamWriter.write(merger, numStats, os);
                } catch (final IOException e) {
                    throw e;
                } finally {
                    Closeables2.closeQuietly(merger, log);
                }
                return null;
            }
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
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, IOException>() {
            public Void apply(final ImhotepSession session) throws IOException {
                final int numStats = getSessionManager().getNumStats(sessionId);
                final FTGSIterator merger = session.getFTGSIteratorSplit(intFields, stringFields, splitIndex, numSplits, termLimit);
                sendSuccessResponse(os);
                return writeFTGSIteratorToOutputStream(numStats, merger, os);
            }
        });
    }

    @Override
    public void handleGetFTGSIteratorSplitNative(final String sessionId, final String[] intFields, final String[] stringFields, final OutputStream os, final int splitIndex, final int numSplits, final long termLimit, final Socket socket) throws IOException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, IOException>() {
            @Override
            public Void apply(final ImhotepSession imhotepSession) throws IOException {
                try {
                    sendSuccessResponse(os);
                    os.flush();
                    imhotepSession.writeFTGSIteratorSplit(intFields, stringFields, splitIndex, numSplits, termLimit, socket);
                } catch (final ImhotepOutOfMemoryException e) {
                    throw new RuntimeException(e);
                }
                return null;//returns Void
            }
        });
    }

    @Override
    public void handleGetSubsetFTGSIteratorSplit(final String sessionId, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final OutputStream os, final int splitIndex, final int numSplits) throws IOException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, IOException>() {
            public Void apply(final ImhotepSession session) throws IOException {
                final int numStats = getSessionManager().getNumStats(sessionId);
                final FTGSIterator merger = session.getSubsetFTGSIteratorSplit(intFields, stringFields, splitIndex, numSplits);
                sendSuccessResponse(os);
                return writeFTGSIteratorToOutputStream(numStats, merger, os);
            }
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
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, IOException>() {
            public Void apply(final ImhotepSession session) throws IOException {
                final int numStats = getSessionManager().getNumStats(sessionId);
                final FTGSIterator merger = session.mergeFTGSSplit(intFields, stringFields, sessionId, nodes, splitIndex, termLimit, sortStat);
                sendSuccessResponse(os);
                return writeFTGSIteratorToOutputStream(numStats, merger, os);
            }
        });
    }

    @Override
    public void handleMergeSubsetFTGSIteratorSplit(final String sessionId, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final OutputStream os, final InetSocketAddress[] nodes, final int splitIndex) throws IOException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, IOException>() {
            public Void apply(final ImhotepSession session) throws IOException {
                final int numStats = getSessionManager().getNumStats(sessionId);
                final FTGSIterator merger = session.mergeSubsetFTGSSplit(intFields, stringFields, sessionId, nodes, splitIndex);
                sendSuccessResponse(os);
                return writeFTGSIteratorToOutputStream(numStats, merger, os);
            }
        });
    }

    @Override
    public void handleGetDocIterator(
            final String sessionId,
            final String[] intFields,
            final String[] stringFields,
            final OutputStream os)
            throws ImhotepOutOfMemoryException, IOException {
        final SharedReference<ImhotepSession> sessionRef = getSessionManager().getSession(sessionId);
        try {
            final DocIterator iterator = sessionRef.get().getDocIterator(intFields, stringFields);
            sendSuccessResponse(os);
            final Future<?> future = ftgsExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws IOException {
                    try {
                        DocOutputStreamWriter.writeNotThreadSafe(iterator, intFields.length, stringFields.length, os);
                    } finally {
                        Closeables2.closeAll(log, iterator, sessionRef);
                    }
                    return null;
                }
            });
            try {
                // do a timed get so the task doesn't run infinitely
                future.get(30L, TimeUnit.MINUTES);
            } catch (final TimeoutException e) {
                future.cancel(true);
                throw Throwables.propagate(e);
            } catch (final ExecutionException | InterruptedException e) {
                throw Throwables.propagate(e);
            }
        } catch (final Throwable t) {
            Closeables2.closeQuietly(sessionRef, log);
            throw Throwables2.propagate(t, ImhotepOutOfMemoryException.class, IOException.class);
        }
    }

    @Override
    public abstract List<ShardInfo> handleGetShardList();

    @Override
    public abstract List<DatasetInfo> handleGetDatasetList();

    @Override
    public abstract ImhotepStatusDump handleGetStatusDump(final boolean includeShardList);

    private static interface ThrowingFunction<A, B, T extends Throwable> {
        B apply(A a) throws T;
    }

    public <Z, T extends Throwable> Z doWithSession(
            final String sessionId,
            final ThrowingFunction<ImhotepSession, Z, T> f) throws T {
        final SharedReference<ImhotepSession> sessionRef = getSessionManager().getSession(sessionId);
        try {
            return f.apply(sessionRef.get());
        } finally {
            Closeables2.closeQuietly(sessionRef, log);
        }
    }

    public <Z> Z doWithSession(final String sessionId, final Function<ImhotepSession, Z> f)  {
        final SharedReference<ImhotepSession> sessionRef = getSessionManager().getSession(sessionId);
        try {
            return f.apply(sessionRef.get());
        } finally {
            Closeables2.closeQuietly(sessionRef, log);
        }
    }

    @Override
    public int handleRegroup(final String sessionId, final GroupRemapRule[] remapRules) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Integer, ImhotepOutOfMemoryException>() {
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.regroup(remapRules);
            }
        });
    }

    public int handleRegroup(final String sessionId, final int numRemapRules, final Iterator<GroupRemapRule> remapRules) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Integer, ImhotepOutOfMemoryException>() {
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.regroup2(numRemapRules, remapRules);
            }
        });
    }

    @Override
    public int handleMultisplitRegroup(final String sessionId, final GroupMultiRemapRule[] remapRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Integer, ImhotepOutOfMemoryException>() {
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.regroup(remapRules, errorOnCollisions);
            }
        });
    }

    @Override
    public int handleMultisplitRegroup(final String sessionId, final int numRemapRules, final Iterator<GroupMultiRemapRule> remapRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Integer, ImhotepOutOfMemoryException>() {
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.regroup(numRemapRules, remapRules, errorOnCollisions);
            }
        });
    }

    @Override
    public int handleQueryRegroup(final String sessionId, final QueryRemapRule remapRule) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Integer, ImhotepOutOfMemoryException>() {
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.regroup(remapRule);
            }
        });
    }

    @Override
    public void handleIntOrRegroup(final String sessionId, final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void handleStringOrRegroup(final String sessionId, final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void handleRandomRegroup(final String sessionId, final String field, final boolean isIntField, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.randomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void handleRandomMultiRegroup(final String sessionId, final String field, final boolean isIntField, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.randomMultiRegroup(field, isIntField, salt, targetGroup, percentages, resultGroups);
                return null;
            }
        });
    }

    @Override
    public void handleRegexRegroup(final String sessionId, final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public int handleMetricRegroup(final String sessionId, final int stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Integer, ImhotepOutOfMemoryException>() {
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.metricRegroup(stat, min, max, intervalSize, noGutters);
            }
        });
    }

    @Override
    public int handleMetricRegroup2D(final String sessionId, final int xStat, final long xMin, final long xMax, final long xIntervalSize, final int yStat, final long yMin, final long yMax, final long yIntervalSize) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Integer, ImhotepOutOfMemoryException>() {
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.metricRegroup2D(xStat, xMin, xMax, xIntervalSize, yStat, yMin, yMax, yIntervalSize);
            }
        });
    }

    public int handleMetricFilter(
            final String sessionId,
            final int stat,
            final long min,
            final long max,
            final boolean negate) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Integer, ImhotepOutOfMemoryException>() {
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.metricFilter(stat, min, max, negate);
            }
        });
    }

    @Override
    public List<TermCount> handleApproximateTopTerms(final String sessionId, final String field, final boolean isIntField, final int k) {
        return doWithSession(sessionId, new Function<ImhotepSession,List<TermCount>>() {
            public List<TermCount> apply(final ImhotepSession session) {
                return session.approximateTopTerms(field, isIntField, k);
            }
        });
    }

    @Override
    public int handlePushStat(final String sessionId, final String metric) throws ImhotepOutOfMemoryException {
        return doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Integer, ImhotepOutOfMemoryException>() {
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                final int newNumStats = session.pushStat(metric);
                getSessionManager().setNumStats(sessionId, newNumStats);
                return newNumStats;
            }
        });
    }

    @Override
    public int handlePopStat(final String sessionId) {
        return doWithSession(sessionId, new Function<ImhotepSession, Integer>() {
            public Integer apply(final ImhotepSession session) {
                final int newNumStats = session.popStat();
                getSessionManager().setNumStats(sessionId, newNumStats);
                return newNumStats;
            }
        });
    }

    @Override
    public int handleGetNumGroups(final String sessionId) {
        return doWithSession(sessionId, new Function<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession imhotepSession) {
                return imhotepSession.getNumGroups();
            }
        });
    }

    @Override
    public void handleCreateDynamicMetric(final String sessionId, final String dynamicMetricName) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.createDynamicMetric(dynamicMetricName);
                return null;
            }
        });
    }

    @Override
    public void handleUpdateDynamicMetric(final String sessionId, final String dynamicMetricName, final int[] deltas) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.updateDynamicMetric(dynamicMetricName, deltas);
                return null;
            }
        });
    }

    @Override
    public void handleConditionalUpdateDynamicMetric(final String sessionId, final String dynamicMetricName, final RegroupCondition[] conditions, final int[] deltas) {
        doWithSession(sessionId, new Function<ImhotepSession, Void>() {
            public Void apply(final ImhotepSession session) {
                session.conditionalUpdateDynamicMetric(dynamicMetricName, conditions, deltas);
                return null;
            }
        });
    }

    @Override
    public void handleGroupConditionalUpdateDynamicMetric(final String sessionId, final String dynamicMetricName, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        doWithSession(sessionId, new Function<ImhotepSession, Void>() {
            public Void apply(final ImhotepSession session) {
                session.groupConditionalUpdateDynamicMetric(dynamicMetricName, groups, conditions, deltas);
                return null;
            }
        });
    }

    public void handleGroupQueryUpdateDynamicMetric(final String sessionId, final String dynamicMetricName, final int[] groups, final Query[] queries, final int[] deltas) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.groupQueryUpdateDynamicMetric(dynamicMetricName, groups, queries, deltas);
                return null;
            }
        });
    }

    @Override
    public void handleRebuildAndFilterIndexes(final String sessionId, final String[] intFields, final String[] stringFields) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.rebuildAndFilterIndexes(Arrays.asList(intFields), Arrays.asList(stringFields));
                return null;
            }
        });
    }

    @Override
    public void handleResetGroups(final String sessionId) throws ImhotepOutOfMemoryException {
        doWithSession(sessionId, new ThrowingFunction<ImhotepSession, Void, ImhotepOutOfMemoryException>() {
            public Void apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.resetGroups();
                return null;
            }
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
            boolean useNativeFtgs,
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
