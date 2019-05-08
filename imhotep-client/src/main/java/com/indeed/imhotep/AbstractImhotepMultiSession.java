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
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.imhotep.exceptions.QueryCancelledException;
import com.indeed.imhotep.scheduling.ImhotepTask;
import com.indeed.imhotep.scheduling.SchedulerType;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.threads.LogOnUncaughtExceptionHandler;
import io.opentracing.ActiveSpan;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jsgroth
 */
public abstract class AbstractImhotepMultiSession<T extends AbstractImhotepSession>
    extends AbstractImhotepSession
    implements Instrumentation.Provider {

    private static final Logger log = Logger.getLogger(AbstractImhotepMultiSession.class);

    private final Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    protected final T[] sessions;

    private final Long[] totalDocFreqBuf;

    protected final Integer[] integerBuf;

    private final Long[] longBuf;

    protected final Object[] nullBuf;

    private final List<TermCount>[] termCountListBuf;

    protected final AtomicLong tempFileSizeBytesLeft;
    private long savedTempFileSizeValue;
    @Nonnull
    private final String userName;
    @Nonnull
    private final String clientName;

    private final SlotTiming slotTiming = new SlotTiming();

    private long savedCPUTime;

    private boolean closed = false;

    /** bytes downloaded from other daemons in peer to peer cache */
    private final AtomicLong downloadedBytesInPeerToPeerCache;

    public final class RelayObserver implements Instrumentation.Observer {
        public synchronized void onEvent(final Instrumentation.Event event) {
            instrumentation.fire(event);
        }
    }

    private final class SessionThreadFactory extends InstrumentedThreadFactory {

        private final String namePrefix;

        SessionThreadFactory(final String namePrefix) {
            this.namePrefix = namePrefix;
            addObserver(new Observer());
        }

        public Thread newThread(@Nonnull final Runnable runnable) {
            final LogOnUncaughtExceptionHandler handler =
                new LogOnUncaughtExceptionHandler(log);
            final Thread result = super.newThread(runnable);
            result.setDaemon(true);
            result.setName(namePrefix + "-" + result.getId());
            result.setUncaughtExceptionHandler(handler);
            return result;
        }

        /* Forward any events from the thread factory to observers of the
           multisession. */
        private final class Observer implements Instrumentation.Observer {
            public void onEvent(final Instrumentation.Event event) {
                event.getProperties().put(Instrumentation.Keys.THREAD_FACTORY, namePrefix);
                AbstractImhotepMultiSession.this.instrumentation.fire(event);
            }
        }
    }

    private final List<InstrumentedThreadFactory> threadFactories;
    protected final ExecutorService executor;
    protected final ExecutorService getSplitBufferThreads;
    protected final ExecutorService mergeSplitBufferThreads;

    protected int numStats = 0;

    @SuppressWarnings({"unchecked"})
    protected AbstractImhotepMultiSession(final String sessionId,
                                          final T[] sessions,
                                          final AtomicLong tempFileSizeBytesLeft,
                                          @Nonnull final String userName,
                                          @Nonnull final String clientName) {
        super(sessionId);
        this.tempFileSizeBytesLeft = tempFileSizeBytesLeft;
        this.savedTempFileSizeValue = (tempFileSizeBytesLeft == null) ? 0 : tempFileSizeBytesLeft.get();
        this.downloadedBytesInPeerToPeerCache = new AtomicLong();
        this.userName = userName;
        this.clientName = clientName;
        if (sessions == null || sessions.length == 0) {
            throw newIllegalArgumentException("at least one session is required");
        }
        for (final AbstractImhotepSession session : sessions) {
            if (!sessionId.equals(session.getSessionId())) {
                throw newIllegalArgumentException("SessionId mismatch in multi session sub-sessions");
            }
        }

        this.sessions = sessions;

        final String threadNamePrefix = "[" + sessionId + "]" + AbstractImhotepMultiSession.class.getName();
        final SessionThreadFactory localThreadFactory = new SessionThreadFactory(threadNamePrefix + "-localThreads");
        final SessionThreadFactory splitThreadFactory = new SessionThreadFactory(threadNamePrefix + "-splitThreads");
        final SessionThreadFactory mergeThreadFactory = new SessionThreadFactory(threadNamePrefix + "-mergeThreads");

        threadFactories =
                new ArrayList<>(Arrays.asList(localThreadFactory,
                        splitThreadFactory,
                        mergeThreadFactory));

        executor = Executors.newCachedThreadPool(localThreadFactory);
        getSplitBufferThreads = Executors.newCachedThreadPool(splitThreadFactory);
        mergeSplitBufferThreads = Executors.newCachedThreadPool(mergeThreadFactory);

        totalDocFreqBuf = new Long[sessions.length];
        integerBuf = new Integer[sessions.length];
        longBuf = new Long[sessions.length];
        nullBuf = new Object[sessions.length];
        termCountListBuf = new List[sessions.length];
    }

    public boolean isClosed() {
        return closed;
    }

    public void addObserver(final Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    public void removeObserver(final Instrumentation.Observer observer) {
        instrumentation.removeObserver(observer);
    }

    @Override
    public long getTotalDocFreq(final String[] intFields, final String[] stringFields) {
        executeRuntimeException(totalDocFreqBuf, new ThrowingFunction<ImhotepSession, Long>() {
            @Override
            public Long apply(final ImhotepSession session) {
                return session.getTotalDocFreq(intFields, stringFields);
            }
        });
        long sum = 0L;
        for (final long totalDocFreq : totalDocFreqBuf) {
            sum += totalDocFreq;
        }
        return sum;
    }

    @Override
    public int regroup(final GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.regroup(rawRules);
            }
        });

        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.regroup(rule);
            }
        });

        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public void regexRegroup(final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void randomRegroup(final String field, final boolean isIntField, final String salt, final double p, final int targetGroup,
                              final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.randomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void randomMultiRegroup(final String field, final boolean isIntField, final String salt, final int targetGroup,
                                   final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.randomMultiRegroup(field, isIntField, salt, targetGroup, percentages, resultGroups);
                return null;
            }
        });
    }

    @Override
    public void randomMetricRegroup(final List<String> stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, (ThrowingFunction<ImhotepSession, Object>) session -> {
            session.randomMetricRegroup(stat, salt, p, targetGroup, negativeGroup, positiveGroup);
            return null;
        });
    }

    @Override
    public void randomMetricMultiRegroup(final List<String> stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, (ThrowingFunction<ImhotepSession, Object>) session -> {
            session.randomMetricMultiRegroup(stat, salt, targetGroup, percentages, resultGroups);
            return null;
        });
    }

    @Override
    public int metricRegroup(final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.metricRegroup(stat, min, max, intervalSize, noGutters);
            }
        });

        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public int metricFilter(final List<String> stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.metricFilter(stat, min, max, negate);
            }
        });

        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public int metricFilter(final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.metricFilter(stat, min, max, targetGroup, negativeGroup, positiveGroup);
            }
        });

        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public List<TermCount> approximateTopTerms(final String field, final boolean isIntField, final int k) {
        final int subSessionK = k * 2;

        executeRuntimeException(termCountListBuf, new ThrowingFunction<ImhotepSession, List<TermCount>>() {
            @Override
            public List<TermCount> apply(final ImhotepSession session) {
                return session.approximateTopTerms(field, isIntField, subSessionK);
            }
        });

        return mergeTermCountLists(termCountListBuf, field, isIntField, k);
    }

    private static List<TermCount> mergeTermCountLists(
            final List<TermCount>[] termCountListBuf,
            final String field,
            final boolean isIntField,
            final int k) {
        final List<TermCount> ret;
        if (isIntField) {
            final Long2LongMap counts = new Long2LongOpenHashMap(k * 2);
            for (final List<TermCount> list : termCountListBuf) {
                for (final TermCount termCount : list) {
                    final long termVal = termCount.getTerm().getTermIntVal();
                    final long count = termCount.getCount();
                    counts.put(termVal, counts.get(termVal) + count);
                }
            }
            ret = Lists.newArrayListWithCapacity(counts.size());
            for (final LongIterator iter = counts.keySet().iterator(); iter.hasNext(); ) {
                final Long term = iter.nextLong();
                final long count = counts.get(term);
                ret.add(new TermCount(new Term(field, true, term, ""), count));
            }
        } else {
            final Object2LongMap<String> counts = new Object2LongOpenHashMap<>(k * 2);
            for (final List<TermCount> list : termCountListBuf) {
                for (final TermCount termCount : list) {
                    final String termVal = termCount.getTerm().getTermStringVal();
                    final long count = termCount.getCount();
                    counts.put(termVal, counts.getLong(termVal) + count);
                }
            }
            ret = Lists.newArrayListWithCapacity(counts.size());
            for (final Map.Entry<String, Long> stringLongEntry : counts.entrySet()) {
                final long count = stringLongEntry.getValue();
                ret.add(new TermCount(new Term(field, false, 0, stringLongEntry.getKey()), count));
            }
        }
        Collections.sort(ret, TermCount.REVERSE_COUNT_COMPARATOR);
        final int end = Math.min(k, ret.size());
        return ret.subList(0, end);
    }

    @Override
    public int pushStat(final String statName) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.pushStat(statName);
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        executeRuntimeException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
                return session.pushStats(statNames);
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int popStat() {
        executeRuntimeException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession session) {
                return session.popStat();
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int getNumStats() {
        // We don't lock cpu for this operation since it's trivial getter
        executeRuntimeExceptionNoCpuLock(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession session) {
                return session.getNumStats();
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int getNumGroups() {
        // We don't lock cpu for this operation since it's
        // trivial getter for local sessions
        // and cpu-cheap request for remote sessions
        executeRuntimeExceptionNoCpuLock(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(final ImhotepSession imhotepSession) {
                return imhotepSession.getNumGroups();
            }
        });
        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public void createDynamicMetric(final String name) throws ImhotepOutOfMemoryException {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
                imhotepSession.createDynamicMetric(name);
                return null;
            }
        });
    }

    @Override
    public void updateDynamicMetric(final String name, final int[] deltas) {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
                imhotepSession.updateDynamicMetric(name, deltas);
                return null;
            }
        });
    }

    @Override
    public void conditionalUpdateDynamicMetric(final String name, final RegroupCondition[] conditions, final int[] deltas) {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession imhotepSession) {
                imhotepSession.conditionalUpdateDynamicMetric(name, conditions, deltas);
                return null;
            }
        });
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(final String name, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession imhotepSession) {
                imhotepSession.groupConditionalUpdateDynamicMetric(name, groups, conditions, deltas);
                return null;
            }
        });
    }

    @Override
    public void groupQueryUpdateDynamicMetric(final String name, final int[] groups, final Query[] conditions, final int[] deltas) {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
                imhotepSession.groupQueryUpdateDynamicMetric(name, groups, conditions, deltas);
                return null;
            }
        });
    }

    private int validateNumStats(final Integer[] numStatBuf) {
        final int newNumStats = numStatBuf[0];
        for (int i = 1; i < numStatBuf.length; ++i) {
            if (numStatBuf[i] != newNumStats) {
                throw newRuntimeException("bug, one session did not return the same number of stats as the others");
            }
        }
        return newNumStats;
    }

    @Override
    public void rebuildAndFilterIndexes(final List<String> intFields,
                                final List<String> stringFields)
        throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
                imhotepSession.rebuildAndFilterIndexes(intFields, stringFields);
                return null;
          }
        });
    }

    @Override
    public final void close() {
        closeWithOptionalPerformanceStats(false);
    }

    @Nullable
    private PerformanceStats[] closeWithOptionalPerformanceStats(final boolean getPerformanceStats) {
        if (closed) {
            return null;
        }
        closed = true;
        final PerformanceStats[] perSessionStats;
        try {
            preClose();
        } finally {
            try {
                if(getPerformanceStats) {
                    perSessionStats = new PerformanceStats[sessions.length];
                    executeRuntimeExceptionNoCpuLock(perSessionStats, ImhotepSession::closeAndGetPerformanceStats);
                } else {
                    perSessionStats = null;
                    executeRuntimeExceptionNoCpuLock(nullBuf, imhotepSession -> { imhotepSession.close(); return null;});
                }
            } finally {
                postClose();
            }
        }
        return perSessionStats;
    }

    @Override
    public void resetGroups() {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(final ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
                imhotepSession.resetGroups();
                return null;
            }
        });
    }

    @Override
    public long getNumDocs() {
        long numDocs = 0;
        for(final ImhotepSession session: sessions) {
            numDocs += session.getNumDocs();
        }
        return numDocs;
    }

    @Override
    public PerformanceStats getPerformanceStats(final boolean reset) {
        final PerformanceStats[] stats = new PerformanceStats[sessions.length];
        executeRuntimeException(stats, imhotepSession -> imhotepSession.getPerformanceStats(reset));

        return combinePerformanceStats(reset, stats);
    }

    // Combination rules are different for local sessions vs what is done in RemoteImhotepMultiSession for remote sessions
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
        long cpuTotalTime = builder.getCpuTime();
        for (final InstrumentedThreadFactory factory: threadFactories) {
            final InstrumentedThreadFactory.PerformanceStats factoryPerformanceStats = factory.getPerformanceStats();
            if (factoryPerformanceStats != null) {
                cpuTotalTime += factoryPerformanceStats.cpuTotalTime;
            }
        }
        builder.setCpuTime(cpuTotalTime - savedCPUTime);
        builder.setCustomStat("downloadedBytesInP2P", downloadedBytesInPeerToPeerCache.get());

        // All sessions share the same AtomicLong tempFileSizeBytesLeft,
        // so value accumulated in builder::ftgsTempFileSize is wrong
        // Calculating and setting correct value.
        final long tempFileSize = (tempFileSizeBytesLeft == null)? 0 : tempFileSizeBytesLeft.get();
        builder.setFtgsTempFileSize(savedTempFileSizeValue - tempFileSize);
        slotTiming.writeToPerformanceStats(builder);
        if (reset) {
           savedTempFileSizeValue = tempFileSize;
           savedCPUTime = cpuTotalTime;
           slotTiming.reset();
        }

        return builder.build();
    }

    @Nonnull
    public String getUserName() {
        return userName;
    }

    @Nonnull
    public String getClientName() {
        return clientName;
    }

    @Nonnull
    public SlotTiming getSlotTiming() {
        return slotTiming;
    }

    public void schedulerExecTimeCallback(SchedulerType schedulerType, long execTime) {
        slotTiming.schedulerExecTimeCallback(schedulerType, execTime);
    }

    public void schedulerWaitTimeCallback(SchedulerType schedulerType, long waitTime) {
        slotTiming.schedulerWaitTimeCallback(schedulerType, waitTime);
    }

    public void addDownloadedBytesInPeerToPeerCache(final long bytesDownloaded) {
        downloadedBytesInPeerToPeerCache.addAndGet(bytesDownloaded);
    }

    @Override
    public PerformanceStats closeAndGetPerformanceStats() {
        final PerformanceStats[] perSessionStats = closeWithOptionalPerformanceStats(true);
        return combinePerformanceStats(false, perSessionStats);
    }

    protected void preClose() {
        for (final InstrumentedThreadFactory factory: threadFactories) {
            try {
                factory.close();
            }
            catch (final IOException e) {
                log.warn("[" + getSessionId() + "]", e);
            }
        }
        getSplitBufferThreads.shutdown();
        mergeSplitBufferThreads.shutdown();
    }

    protected void postClose() {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                log.warn("[" + getSessionId() + "]executor did not shut down, continuing anyway");
            }
        } catch (final InterruptedException e) {
            log.warn("[" + getSessionId() + "]" + e);
        }
    }

    protected <R> void executeRuntimeException(final R[] ret, final ThrowingFunction<? super T, ? extends R> function) {
        executeRuntimeException(ret, true, function);
    }

    protected <R> void executeRuntimeExceptionNoCpuLock(final R[] ret, final ThrowingFunction<? super T, ? extends R> function) {
        executeRuntimeException(ret, false, function);
    }

    private <R> void executeRuntimeException(final R[] ret,
                                               final boolean lockCPU,
                                               final ThrowingFunction<? super T, ? extends R> function) {
        try {
            executeSessions(ret, lockCPU, function);
        } catch (final ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    protected <R> void executeMemoryException(final R[] ret, final ThrowingFunction<? super T, ? extends R> function) throws ImhotepOutOfMemoryException {
        try {
            executeSessions(ret, true, function);
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause();
            if (closed && cause instanceof ClosedByInterruptException) {
                throw newQueryCancelledException(cause);
            } else if (cause instanceof ImhotepOutOfMemoryException) {
                throw newImhotepOutOfMemoryException(cause);
            } else {
                throw newRuntimeException(cause);
            }
        }
    }

    protected final <E, T> void execute(final ExecutorService es,
                                        final T[] ret, final E[] things, final boolean lockCPU,
                                        final ThrowingFunction<? super E, ? extends T> function)
            throws ExecutionException {
        final Tracer tracer = GlobalTracer.get();
        final RequestContext requestContext = RequestContext.THREAD_REQUEST_CONTEXT.get();
        final List<Future<T>> futures = new ArrayList<>(things.length);
        try (final ActiveSpan activeSpan = tracer.buildSpan("execute").withTag("sessionid", getSessionId()).startActive()) {
            try {
                for (final E thing : things) {
                    final ActiveSpan.Continuation continuation = activeSpan.capture();
                    futures.add(es.submit(() -> {
                        RequestContext.THREAD_REQUEST_CONTEXT.set(requestContext);
                        // this must happen after setting request context
                        ImhotepTask.setup(AbstractImhotepMultiSession.this);
                        try (final ActiveSpan parentSpan = continuation.activate()) {
                            if (lockCPU) {
                                try (final Closeable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                                    return function.apply(thing);
                                }
                            } else {
                                return function.apply(thing);
                            }
                        } finally {
                            ImhotepTask.clear();
                        }
                    }));
                }
            } catch (final RejectedExecutionException e) {
                safeClose();
                throw new QueryCancelledException("The query was cancelled during execution", e);
            }

            Throwable t = null;
            for (int i = 0; i < futures.size(); ++i) {
                try {
                    final Future<T> future = futures.get(i);
                    ret[i] = future.get();
                } catch (final Throwable t2) {
                    t = t2;
                }
            }
            if (t != null) {
                safeClose();
                Throwables.propagateIfInstanceOf(t.getCause(), ImhotepKnownException.class);
                Throwables.propagateIfInstanceOf(t, ExecutionException.class);
                throw Throwables.propagate(t);
            }
        }
    }

    protected final <E, T> void execute(final T[] ret, final E[] things, final boolean lockCPU,
                                        final ThrowingFunction<? super E, ? extends T> function)
        throws ExecutionException {
        execute(executor, ret, things, lockCPU, function);
    }

    protected <R> void executeSessions(final ExecutorService es,
                                       final R[] ret, final boolean lockCPU,
                                       final ThrowingFunction<? super T, ? extends R> function)
        throws ExecutionException {
        execute(es, ret, sessions, lockCPU, registerSessionWrapper(function));
    }

    protected <R> void executeSessions(final R[] ret, final boolean lockCPU,
                                       final ThrowingFunction<? super T, ? extends R> function)
        throws ExecutionException {
        execute(executor, ret, sessions, lockCPU, registerSessionWrapper(function));
    }

    private <R> ThrowingFunction<? super T, ? extends R> registerSessionWrapper(final ThrowingFunction<? super T, ? extends R> function) {
        return (T input) -> {
            ImhotepTask.registerInnerSession(input);
            return function.apply(input);
        };
    }

    protected interface ThrowingFunction<K, V> {
        V apply(K k) throws Exception;
    }

    protected void safeClose() {
        try {
            close();
        } catch (final Exception e) {
            log.error("[" + getSessionId() + "]error closing session", e);
        }
    }
}
