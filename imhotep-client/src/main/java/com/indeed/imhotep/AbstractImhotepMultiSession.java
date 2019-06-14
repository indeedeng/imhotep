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
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.imhotep.exceptions.QueryCancelledException;
import com.indeed.imhotep.protobuf.Operator;
import com.indeed.imhotep.scheduling.ImhotepTask;
import com.indeed.imhotep.scheduling.SchedulerType;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.threads.LogOnUncaughtExceptionHandler;
import io.opentracing.ActiveSpan;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import lombok.Setter;
import lombok.experimental.Accessors;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

    protected final Void[] nullBuf;

    private final List<TermCount>[] termCountListBuf;

    protected final AtomicLong tempFileSizeBytesLeft;
    private long savedTempFileSizeValue;
    @Nonnull
    private final String userName;
    @Nonnull
    private final String clientName;

    private final byte priority;

    private final SlotTiming slotTiming = new SlotTiming();

    private long savedCPUTime;

    private boolean closed = false;

    /** bytes downloaded from other daemons in peer to peer cache */
    private final AtomicLong downloadedBytesP2P;

    /** bytes downloaded in the filesystem from remote file store */
    private final AtomicLong downloadedBytes;

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
                                          @Nonnull final String clientName,
                                          final byte priority) {
        super(sessionId);
        this.tempFileSizeBytesLeft = tempFileSizeBytesLeft;
        this.savedTempFileSizeValue = (tempFileSizeBytesLeft == null) ? 0 : tempFileSizeBytesLeft.get();
        this.downloadedBytesP2P = new AtomicLong();
        this.downloadedBytes = new AtomicLong();
        this.userName = userName;
        this.clientName = clientName;
        this.priority = priority;
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
        nullBuf = new Void[sessions.length];
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
        executor().executeRuntimeException(totalDocFreqBuf, session -> session.getTotalDocFreq(intFields, stringFields));
        long sum = 0L;
        for (final long totalDocFreq : totalDocFreqBuf) {
            sum += totalDocFreq;
        }
        return sum;
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        executor().executeMemoryException(integerBuf, session -> session.regroup(regroupParams, rule));
        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public void regexRegroup(final RegroupParams regroupParams, final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executor().executeMemoryExceptionVoid(nullBuf, session -> session.regexRegroup(regroupParams, field, regex, targetGroup, negativeGroup, positiveGroup));
    }

    @Override
    public void randomRegroup(final RegroupParams regroupParams, final String field, final boolean isIntField, final String salt, final double p, final int targetGroup,
                              final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executor().executeMemoryExceptionVoid(nullBuf, session -> session.randomRegroup(regroupParams, field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup));
    }

    @Override
    public void randomMultiRegroup(final RegroupParams regroupParams, final String field, final boolean isIntField, final String salt, final int targetGroup,
                                   final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        executor().executeMemoryExceptionVoid(nullBuf, session -> session.randomMultiRegroup(regroupParams, field, isIntField, salt, targetGroup, percentages, resultGroups));
    }

    @Override
    public void randomMetricRegroup(final RegroupParams regroupParams, final List<String> stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executor().executeMemoryExceptionVoid(nullBuf, session -> session.randomMetricRegroup(regroupParams, stat, salt, p, targetGroup, negativeGroup, positiveGroup));
    }

    @Override
    public void randomMetricMultiRegroup(final RegroupParams regroupParams, final List<String> stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        executor().executeMemoryExceptionVoid(nullBuf, session -> session.randomMetricMultiRegroup(regroupParams, stat, salt, targetGroup, percentages, resultGroups));
    }

    @Override
    public int metricRegroup(final RegroupParams regroupParams, final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        executor().executeMemoryException(integerBuf, session -> session.metricRegroup(regroupParams, stat, min, max, intervalSize, noGutters));
        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public int metricFilter(final RegroupParams regroupParams, final List<String> stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        executor().executeMemoryException(integerBuf, (ThrowingFunction<ImhotepSession, Integer>) session -> session.metricFilter(regroupParams, stat, min, max, negate));
        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public int metricFilter(final RegroupParams regroupParams, final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executor().executeMemoryException(integerBuf, (ThrowingFunction<ImhotepSession, Integer>) session -> session.metricFilter(regroupParams, stat, min, max, targetGroup, negativeGroup, positiveGroup));
        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public List<TermCount> approximateTopTerms(final String field, final boolean isIntField, final int k) {
        final int subSessionK = k * 2;
        executor().executeRuntimeException(termCountListBuf, (ThrowingFunction<ImhotepSession, List<TermCount>>) session -> session.approximateTopTerms(field, isIntField, subSessionK));
        return mergeTermCountLists(termCountListBuf, field, isIntField, k);
    }

    @Override
    public void consolidateGroups(final List<String> inputGroups, final Operator operation, final String outputGroups) throws ImhotepOutOfMemoryException {
        executor().executeMemoryExceptionVoid(nullBuf, session -> session.consolidateGroups(inputGroups, operation, outputGroups));
    }

    @Override
    public void deleteGroups(final List<String> groupsToDelete) {
        executor().executeRuntimeExceptionVoid(nullBuf, session -> session.deleteGroups(groupsToDelete));
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
        executor().executeMemoryException(integerBuf, session -> session.pushStat(statName));
        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        executor().executeRuntimeException(integerBuf, session -> session.pushStats(statNames));
        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int popStat() {
        executor().executeRuntimeException(integerBuf, ImhotepSession::popStat);
        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int getNumStats() {
        // We don't lock cpu for this operation since it's trivial getter
        executor().lockCPU(false).executeRuntimeException(integerBuf, ImhotepSession::getNumStats);
        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int getNumGroups(final String groupsName) {
        // We don't lock cpu for this operation since it's
        // trivial getter for local sessions
        // and cpu-cheap request for remote sessions
        executor().lockCPU(false).executeRuntimeException(integerBuf, imhotepSession -> imhotepSession.getNumGroups(groupsName));
        return Collections.max(Arrays.asList(integerBuf));
    }

    @Override
    public void createDynamicMetric(final String name) throws ImhotepOutOfMemoryException {
        executor().executeMemoryExceptionVoid(nullBuf, imhotepSession -> imhotepSession.createDynamicMetric(name));
    }

    @Override
    public void updateDynamicMetric(final String groupsName, final String name, final int[] deltas) {
        executor().executeRuntimeExceptionVoid(nullBuf, imhotepSession -> imhotepSession.updateDynamicMetric(groupsName, name, deltas));
    }

    @Override
    public void conditionalUpdateDynamicMetric(final String name, final RegroupCondition[] conditions, final int[] deltas) {
        executor().executeRuntimeExceptionVoid(nullBuf, imhotepSession -> imhotepSession.conditionalUpdateDynamicMetric(name, conditions, deltas));
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(final String groupsName, final String name, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        executor().executeRuntimeExceptionVoid(nullBuf, imhotepSession -> imhotepSession.groupConditionalUpdateDynamicMetric(groupsName, name, groups, conditions, deltas));
    }

    @Override
    public void groupQueryUpdateDynamicMetric(final String groupsName, final String name, final int[] groups, final Query[] conditions, final int[] deltas) {
        executor().executeRuntimeExceptionVoid(nullBuf, imhotepSession -> imhotepSession.groupQueryUpdateDynamicMetric(groupsName, name, groups, conditions, deltas));
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
                    executor().lockCPU(false).executeRuntimeException(perSessionStats, ImhotepSession::closeAndGetPerformanceStats);
                } else {
                    perSessionStats = null;
                    executor().lockCPU(false).executeRuntimeExceptionVoid(nullBuf, ImhotepSession::close);
                }
            } finally {
                postClose();
            }
        }
        return perSessionStats;
    }

    @Override
    public void resetGroups(final String groupsName) throws ImhotepOutOfMemoryException {
        executor().executeMemoryExceptionVoid(nullBuf, imhotepSession -> imhotepSession.resetGroups(groupsName));
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
        executor().executeRuntimeException(stats, imhotepSession -> imhotepSession.getPerformanceStats(reset));
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
        builder.setCustomStat("downloadedBytesP2P", downloadedBytesP2P.get());
        builder.setCustomStat("downloadedBytes", downloadedBytes.get());

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

    public byte getPriority() {
        return priority;
    }

    public void schedulerExecTimeCallback(SchedulerType schedulerType, long execTime) {
        slotTiming.schedulerExecTimeCallback(schedulerType, execTime);
    }

    public void schedulerWaitTimeCallback(SchedulerType schedulerType, long waitTime) {
        slotTiming.schedulerWaitTimeCallback(schedulerType, waitTime);
    }

    public void addDownloadedBytesInPeerToPeerCache(final long bytesDownloaded) {
        downloadedBytesP2P.addAndGet(bytesDownloaded);
    }

    public void addDownloadedBytes(final long bytesDownloaded) {
        downloadedBytes.addAndGet(bytesDownloaded);
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

    private final <E, T> void execute(final ExecutorService es,
                                      final T[] ret, final E[] things, final boolean lockCPU,
                                      final ThrowingFunction<? super E, ? extends T> function,
                                      @Nullable final ThrowingConsumer<T> cleanupFunction)
            throws ExecutionException {
        final Tracer tracer = GlobalTracer.get();
        final RequestContext requestContext = RequestContext.THREAD_REQUEST_CONTEXT.get();
        final List<Future<T>> futures = new ArrayList<>(things.length);
        final StrictCloser closerOnFailure = new StrictCloser();
        try (final ActiveSpan activeSpan = tracer.buildSpan("execute").withTag("sessionid", getSessionId()).startActive()) {
            try {
                for (final E thing : things) {
                    final ActiveSpan.Continuation continuation = activeSpan.capture();
                    futures.add(es.submit(() -> {
                        RequestContext.THREAD_REQUEST_CONTEXT.set(requestContext);
                        // this must happen after setting request context
                        ImhotepTask.setup(AbstractImhotepMultiSession.this);
                        try (final ActiveSpan parentSpan = continuation.activate()) {
                            final T returnValue;
                            if (lockCPU) {
                                try (final Closeable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                                    returnValue = function.apply(thing);
                                }
                            } else {
                                returnValue = function.apply(thing);
                            }
                            if (cleanupFunction != null) {
                                final AtomicBoolean cleanedUp = new AtomicBoolean(false);
                                closerOnFailure.registerOrClose(() -> {
                                    if (!cleanedUp.getAndSet(true)) {
                                        try {
                                            cleanupFunction.applyVoid(returnValue);
                                        } catch (final Exception e) {
                                            log.error("Failed to cleanup", e);
                                        }
                                    }
                                });
                            }
                            return returnValue;
                        } finally {
                            ImhotepTask.clear();
                        }
                    }));
                }
            } catch (final RejectedExecutionException e) {
                Closeables2.closeQuietly(closerOnFailure, log);
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
                Arrays.fill(ret, null);
                Closeables2.closeQuietly(closerOnFailure, log);
                safeClose();
                Throwables.propagateIfInstanceOf(t.getCause(), ImhotepKnownException.class);
                Throwables.propagateIfInstanceOf(t, ExecutionException.class);
                throw Throwables.propagate(t);
            }
        }
    }

    private <R> ThrowingFunction<? super T, ? extends R> registerSessionWrapper(final ThrowingFunction<? super T, ? extends R> function) {
        return (T input) -> {
            ImhotepTask.registerInnerSession(input);
            return function.apply(input);
        };
    }

    protected <R> Executor<R> executor() {
        return new Executor<>();
    }

    protected <R extends Closeable> Executor<R> closeOnFailExecutor() {
        return new Executor<R>().cleanupFunction(Closeable::close);
    }

    @Accessors(fluent = true, chain = true)
    @Setter
    protected class Executor<R> {
        private ExecutorService executorService = executor;
        @Nullable
        private ThrowingConsumer<R> cleanupFunction = null;
        private boolean lockCPU = true;

        public <E> void execute(final R[] ret, final E[] things, final ThrowingFunction<? super E, ? extends R> function) throws ExecutionException {
            AbstractImhotepMultiSession.this.execute(executorService, ret, things, lockCPU, function, cleanupFunction);
        }

        public void executeSessions(final R[] ret, final ThrowingFunction<? super T, ? extends R> function) throws ExecutionException {
            execute(ret, sessions, registerSessionWrapper(function));
        }

        public <E> void executeRuntimeException(final R[] ret, final E[] things, final ThrowingFunction<? super E, ? extends R> function) {
            try {
                execute(ret, things, function);
            } catch (final ExecutionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }

        public void executeRuntimeException(final R[] ret, final ThrowingFunction<? super T, ? extends R> function) {
            executeRuntimeException(ret, sessions, registerSessionWrapper(function));
        }

        public void executeRuntimeExceptionVoid(final R[] ret, final ThrowingConsumer<? super T> consumer) {
            executeRuntimeException(ret, consumer.toFunction());
        }

        public <E> void executeMemoryException(final R[] ret, final E[] things, final ThrowingFunction<? super E, ? extends R> function) throws ImhotepOutOfMemoryException {
            try {
                execute(ret, things, function);
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                if (closed && (cause instanceof ClosedByInterruptException)) {
                    throw newQueryCancelledException(cause);
                }
                if (cause instanceof ImhotepOutOfMemoryException) {
                    throw newImhotepOutOfMemoryException(cause);
                }
                throw newRuntimeException(cause);
            }
        }

        public void executeMemoryException(final R[] ret, final ThrowingFunction<? super T, ? extends R> function) throws ImhotepOutOfMemoryException {
            executeMemoryException(ret, sessions, registerSessionWrapper(function));
        }

        public void executeMemoryExceptionVoid(final R[] ret, final ThrowingConsumer<? super T> consumer) throws ImhotepOutOfMemoryException {
            executeMemoryException(ret, consumer.toFunction());
        }

        public <E> void executeIOException(final R[] ret, final E[] things, final ThrowingFunction<? super E, ? extends R> function) throws IOException {
            try {
                execute(ret, things, function);
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                if (closed && (cause instanceof ClosedByInterruptException)) {
                    throw newQueryCancelledException(cause);
                }
                Throwables.propagateIfInstanceOf(cause, IOException.class);
                throw newRuntimeException(cause);
            }
        }
    }

    protected interface ThrowingFunction<K, V> {
        V apply(K k) throws Exception;
    }

    protected interface ThrowingConsumer<K> {
        void applyVoid(K k) throws Exception;
        default <R> ThrowingFunction<K, R> toFunction() {
            return k -> {
                applyVoid(k);
                return null;
            };
        }
    }

    protected void safeClose() {
        try {
            close();
        } catch (final Exception e) {
            log.error("[" + getSessionId() + "]error closing session", e);
        }
    }
}
