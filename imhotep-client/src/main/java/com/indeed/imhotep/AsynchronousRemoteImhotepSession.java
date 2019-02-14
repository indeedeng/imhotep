package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * An asynchronous wrapper for an ImhotepSession
 *
 * Methods whose primary purpose is to read data (getNumGroups, getGroupStats, getFtgsIterator, ...) are SYNCHRONOUS.
 * Methods whose primary purpose is to modify data (regroup, pushStat, popStat, dynamic metric stuff, ...) are
 *     ASYNCHRONOUS and any data that they normally would return in the ImhotepSession interface has been replaced with
 *     dummy data.
 * close() is SYNCHRONOUS so that try-finally blocks are sensible.
 * The ImhotepSession interface does not allow us to expose the ExecutionExceptions in a checked fashion, so everything
 * everywhere is unchecked, basically.
 */
public class AsynchronousRemoteImhotepSession extends AbstractImhotepSession {
    private final ImhotepRemoteSession wrapped;
    /**
     * Any currently in-flight operations.
     */
    private CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
    private final ExecutorService executor;

    public AsynchronousRemoteImhotepSession(final ImhotepRemoteSession wrapped, final ExecutorService executor) {
        super(wrapped.getSessionId());
        this.wrapped = wrapped;
        this.executor = executor;
    }

    public interface Command {
        void apply(ImhotepRemoteSession session) throws ImhotepOutOfMemoryException;
    }

    /**
     * Add a command to the end of the futures-chain
     */
    private synchronized void doIt(final Command command) {
        future = future.thenRunAsync(() -> {
            try {
                command.apply(this.wrapped);
            } catch (final ImhotepOutOfMemoryException e) {
                throw new RuntimeException("Ran out of Imhotep memory while executing command: " + command, e);
            }
        }, executor);
    }

    /**
     * wait for the futures-chain to complete and then extract some value from the session
     * at that point in time
     */
    private synchronized <T> T extractFromSession(final Function<ImhotepSession, T> function) {
        try {
            future.get();
            return function.apply(this.wrapped);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        } catch (final ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    private interface FunctionMemoryException<T> {
        public T apply(ImhotepSession session) throws ImhotepOutOfMemoryException;
    }

    private synchronized <T> T extractFromSessionMemoryException(final FunctionMemoryException<T> function) throws ImhotepOutOfMemoryException {
        try {
            future.get();
            return function.apply(this.wrapped);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        } catch (final ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), ImhotepOutOfMemoryException.class);
            throw Throwables.propagate(e);
        }
    }

    public void synchronize() {
        extractFromSession(session -> session);
    }

    @Override
    public long getTotalDocFreq(final String[] intFields, final String[] stringFields) {
        return extractFromSession(session -> session.getTotalDocFreq(intFields, stringFields));
    }

    @Override
    public long[] getGroupStats(final List<String> stat) throws ImhotepOutOfMemoryException {
        return extractFromSessionMemoryException(session -> session.getGroupStats(stat));
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final List<String> stat) throws ImhotepOutOfMemoryException {
        return extractFromSessionMemoryException(session -> session.getGroupStatsIterator(stat));
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return extractFromSessionMemoryException(session -> session.getFTGSIterator(intFields, stringFields, stats));
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return extractFromSessionMemoryException(session -> session.getFTGSIterator(intFields, stringFields, termLimit, stats));
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, final int sortStat, final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return extractFromSessionMemoryException(session -> session.getFTGSIterator(intFields, stringFields, termLimit, sortStat, stats));
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return extractFromSessionMemoryException(session -> session.getSubsetFTGSIterator(intFields, stringFields, stats));
    }

    @Override
    public FTGSIterator getFTGSIterator(final FTGSParams params) throws ImhotepOutOfMemoryException {
        return extractFromSessionMemoryException(session -> session.getFTGSIterator(params));
    }

    @Override
    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        return extractFromSession(session -> session.getDistinct(field, isIntField));
    }

    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        doIt(session -> session.regroup(rawRules));
        return -999;
    }

    @Override
    public int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules) throws ImhotepOutOfMemoryException {
        doIt(session -> session.regroup(numRawRules, rawRules));
        return -999;
    }

    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        doIt(session -> session.regroup(rawRules, errorOnCollisions));
        return -999;
    }

    @Override
    public int regroupWithProtos(final GroupMultiRemapMessage[] rawRuleMessages, final boolean errorOnCollisions) {
        doIt(session -> session.regroupWithProtos(rawRuleMessages, errorOnCollisions));
        return -999;
    }

    @Override
    public int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        doIt(session -> session.regroup(numRawRules, rawRules, errorOnCollisions));
        return -999;
    }

    public void regroupWithRuleSender(final RequestTools.GroupMultiRemapRuleSender sender, final boolean errorOnCollisions) {
        doIt(session -> session.regroupWithSender(sender, errorOnCollisions, session.newTimer()));
    }

    @Override
    public int regroup(final GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        doIt(session -> session.regroup(rawRules));
        return -999;
    }

    @Override
    public int regroup2(final int numRawRules, final Iterator<GroupRemapRule> iterator) {
        doIt(session -> session.regroup2(numRawRules, iterator));
        return -999;
    }

    @Override
    public int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        doIt(session -> session.regroup(rule));
        return -999;
    }

    @Override
    public void intOrRegroup(final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) {
        doIt(session -> session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup));
    }

    @Override
    public void stringOrRegroup(final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) {
        doIt(session -> session.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup));
    }

    @Override
    public void regexRegroup(final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup) {
        doIt(session -> session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup));
    }

    @Override
    public void randomRegroup(final String field, final boolean isIntField, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) {
        doIt(session -> session.randomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup));
    }

    @Override
    public void randomMultiRegroup(final String field, final boolean isIntField, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) {
        doIt(session -> session.randomMultiRegroup(field, isIntField, salt, targetGroup, percentages, resultGroups));
    }

    @Override
    public void randomMetricRegroup(final List<String> stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doIt(session -> session.randomMetricRegroup(stat, salt, p, targetGroup, negativeGroup, positiveGroup));
    }

    @Override
    public void randomMetricMultiRegroup(final List<String> stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        doIt(session -> session.randomMetricMultiRegroup(stat, salt, targetGroup, percentages, resultGroups));
    }

    @Override
    public int metricRegroup(final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        doIt(session -> session.metricRegroup(stat, min, max, intervalSize, noGutters));
        return -999;
    }

    @Override
    public int regroup(final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        doIt(session -> session.regroup(fromGroups, toGroups, filterOutNotTargeted));
        return -999;
    }

    @Override
    public int metricFilter(final List<String> stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        doIt(session -> session.metricFilter(stat, min, max, negate));
        return -999;
    }

    @Override
    public int metricFilter(final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        doIt(session -> session.metricFilter(stat, min, max, targetGroup, negativeGroup, positiveGroup));
        return -999;
    }

    @Override
    public List<TermCount> approximateTopTerms(final String field, final boolean isIntField, final int k) {
        return extractFromSession(session -> session.approximateTopTerms(field, isIntField, k));
    }

    @Override
    public int pushStat(final String statName) {
        doIt(session -> session.pushStat(statName));
        return -999;
    }

    @Override
    public int pushStats(final List<String> statNames) {
        doIt(session -> session.pushStats(statNames));
        return -999;
    }

    @Override
    public int popStat() {
        doIt(ImhotepRemoteSession::popStat);
        return -999;
    }

    @Override
    public int getNumStats() {
        return extractFromSession(ImhotepSession::getNumStats);
    }

    @Override
    public int getNumGroups() {
        return extractFromSession(ImhotepSession::getNumGroups);
    }

    @Override
    public long getLowerBound(final int stat) {
        return extractFromSession(session -> session.getLowerBound(stat));
    }

    @Override
    public long getUpperBound(final int stat) {
        return extractFromSession(session -> session.getUpperBound(stat));
    }

    @Override
    public void createDynamicMetric(final String name) {
        doIt(session -> session.createDynamicMetric(name));
    }

    @Override
    public void updateDynamicMetric(final String name, final int[] deltas) {
        doIt(session -> session.updateDynamicMetric(name, deltas));
    }

    @Override
    public void conditionalUpdateDynamicMetric(final String name, final RegroupCondition[] conditions, final int[] deltas) {
        doIt(session -> session.conditionalUpdateDynamicMetric(name, conditions, deltas));
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(final String name, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        doIt(session -> session.groupConditionalUpdateDynamicMetric(name, groups, conditions, deltas));
    }

    @Override
    public void groupQueryUpdateDynamicMetric(final String name, final int[] groups, final Query[] conditions, final int[] deltas) {
        doIt(session -> session.groupQueryUpdateDynamicMetric(name, groups, conditions, deltas));
    }

    @Override
    public void close() {
        // TODO: Is it bad for this to wait for any pending requests to finish?
        //       Should it just call close() abruptly on the `wrapped` field?
        //       Especially considering try-with-resources.
        extractFromSession(session -> session).close();
    }

    @Override
    public void resetGroups() {
        doIt(ImhotepRemoteSession::resetGroups);
    }

    @Override
    public void rebuildAndFilterIndexes(final List<String> intFields, final List<String> stringFields) {
        doIt(session -> session.rebuildAndFilterIndexes(intFields, stringFields));
    }

    @Override
    public long getNumDocs() {
        return extractFromSession(ImhotepSession::getNumDocs);
    }

    @Override
    public PerformanceStats getPerformanceStats(final boolean reset) {
        return extractFromSession(session -> session.getPerformanceStats(reset));
    }

    @Override
    public PerformanceStats closeAndGetPerformanceStats() {
        return extractFromSession(ImhotepSession::closeAndGetPerformanceStats);
    }

    @Override
    public void addObserver(final Instrumentation.Observer observer) {
        doIt(session -> session.addObserver(observer));
    }

    @Override
    public void removeObserver(final Instrumentation.Observer observer) {
        doIt(session -> session.removeObserver(observer));
    }

    @Override
    public String toString() {
        return "AsynchronousRemoteImhotepSession{" +
                "wrapped=" + wrapped +
                '}';
    }
}
