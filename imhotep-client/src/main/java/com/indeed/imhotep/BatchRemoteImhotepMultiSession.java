package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.commands.GetGroupStats;
import com.indeed.imhotep.commands.IntOrRegroup;
import com.indeed.imhotep.commands.MetricRegroup;
import com.indeed.imhotep.commands.MultiRegroup;
import com.indeed.imhotep.commands.MultiRegroupIterator;
import com.indeed.imhotep.commands.MultiRegroupMessagesSender;
import com.indeed.imhotep.commands.QueryRegroup;
import com.indeed.imhotep.commands.RandomMetricMultiRegroup;
import com.indeed.imhotep.commands.RandomMetricRegroup;
import com.indeed.imhotep.commands.RandomMultiRegroup;
import com.indeed.imhotep.commands.RandomRegroup;
import com.indeed.imhotep.commands.RegexRegroup;
import com.indeed.imhotep.commands.StringOrRegroup;
import com.indeed.imhotep.commands.TargetedMetricFilter;
import com.indeed.imhotep.commands.UnconditionalRegroup;
import com.indeed.imhotep.commands.UntargetedMetricFilter;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import it.unimi.dsi.fastutil.longs.LongIterators;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ImhotepSession implementation to send a Batch of Imhotep Requests in a single socket stream.
 * Only requests for Regroups and GetGroupStats are included because of cross daemon synchronization needed for FTGS requests.
 * Therefore the batch request will be of the form ((Regroup)*(GetGroupStats)*)
 */
public class BatchRemoteImhotepMultiSession extends AbstractImhotepSession {

    final RemoteImhotepMultiSession remoteImhotepMultiSession;

    private static final Logger log = Logger.getLogger(BatchRemoteImhotepMultiSession.class);

    private final List<ImhotepCommand> commands = new ArrayList<>();

    private <T> T executeBatchAndGetResult(final ImhotepCommand<T> lastCommand) throws ImhotepOutOfMemoryException {
        try {
            return remoteImhotepMultiSession.processImhotepBatchRequest(commands, lastCommand);
        } finally {
            commands.clear();
        }
    }

    private Void executeBatch() throws ImhotepOutOfMemoryException {
        if (commands.isEmpty()) {
            return null;
        }

        try {
            final ImhotepCommand<?> lastCommand = commands.get(commands.size() - 1);
            commands.remove(commands.size() - 1);
            remoteImhotepMultiSession.processImhotepBatchRequest(commands, lastCommand);
            return null;
        } finally {
            commands.clear();
        }
    }

    public Void executeBatchNoMemoryException() {
        try {
            executeBatch();
            return null;
        } catch (final ImhotepOutOfMemoryException e) {
            log.error("ImhotepOutOfMemoryException while executing Imhotep Batch commands. SessionId: " + getSessionId() + " commands class List: " + getLogCommandClassNameList());
            throw Throwables.propagate(e);
        }
    }

    BatchRemoteImhotepMultiSession(final RemoteImhotepMultiSession remoteImhotepMultiSession) {
        super(remoteImhotepMultiSession.getSessionId());
        this.remoteImhotepMultiSession = remoteImhotepMultiSession;
    }

    @Override
    public long getTotalDocFreq(final String[] intFields, final String[] stringFields) {
        return remoteImhotepMultiSession.getTotalDocFreq(intFields, stringFields);
    }

    @Override
    public long[] getGroupStats(final List<String> stat) throws ImhotepOutOfMemoryException {
        try (final GroupStatsIterator groupStatsIterator = executeBatchAndGetResult(new GetGroupStats(stat, remoteImhotepMultiSession.getSessionId()))) {
            return LongIterators.unwrap(groupStatsIterator, groupStatsIterator.getNumGroups());
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final List<String> stat) throws ImhotepOutOfMemoryException {
        return executeBatchAndGetResult(new GetGroupStats(stat, remoteImhotepMultiSession.getSessionId()));
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        executeBatch();
        return remoteImhotepMultiSession.getSubsetFTGSIterator(intFields, stringFields, stats);
    }

    @Override
    public FTGSIterator getFTGSIterator(final FTGSParams params) throws ImhotepOutOfMemoryException {
        executeBatch();
        return remoteImhotepMultiSession.getFTGSIterator(params);
    }

    @Override
    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        executeBatchNoMemoryException();
        return remoteImhotepMultiSession.getDistinct(field, isIntField);
    }

    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        commands.add(MultiRegroup.createMultiRegroupCommand(rawRules, errorOnCollisions, remoteImhotepMultiSession.getSessionId()));
        return -999;
    }

    @Override
    public int regroupWithProtos(final GroupMultiRemapMessage[] rawRuleMessages, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        commands.add(MultiRegroupMessagesSender.createMultiRegroupMessagesSender(rawRuleMessages, errorOnCollisions, remoteImhotepMultiSession.getSessionId()));
        return -999;
    }

    public int regroupWithRuleSender(final RequestTools.GroupMultiRemapRuleSender ruleSender, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        commands.add(MultiRegroupMessagesSender.createMultiRegroupMessagesSender(ruleSender, errorOnCollisions, remoteImhotepMultiSession.getSessionId()));
        return -999;
    }

    @Override
    public int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        commands.add(new MultiRegroupIterator(numRawRules, rawRules, errorOnCollisions, remoteImhotepMultiSession.getSessionId()));
        return -999;
    }

    @Override
    public int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        commands.add(new QueryRegroup(rule, remoteImhotepMultiSession.getSessionId()));
        return -999;
    }

    @Override
    public void intOrRegroup(final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new IntOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup, remoteImhotepMultiSession.getSessionId()));
    }

    @Override
    public void stringOrRegroup(final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new StringOrRegroup(field, Arrays.asList(terms), targetGroup, negativeGroup, positiveGroup, remoteImhotepMultiSession.getSessionId()));
    }

    @Override
    public void regexRegroup(final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new RegexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup, remoteImhotepMultiSession.getSessionId()));
    }

    @Override
    public void randomRegroup(final String field, final boolean isIntField, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new RandomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup, remoteImhotepMultiSession.getSessionId()));
    }

    @Override
    public void randomMultiRegroup(final String field, final boolean isIntField, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        commands.add(new RandomMultiRegroup(field, isIntField, salt, targetGroup, percentages, resultGroups, remoteImhotepMultiSession.getSessionId()));
    }

    @Override
    public void randomMetricRegroup(final List<String> stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new RandomMetricRegroup(stat, salt, p, targetGroup, negativeGroup, positiveGroup, remoteImhotepMultiSession.getSessionId()));
    }

    @Override
    public void randomMetricMultiRegroup(final List<String> stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        commands.add(new RandomMetricMultiRegroup(stat, salt, targetGroup, percentages, resultGroups, remoteImhotepMultiSession.getSessionId()));
    }

    @Override
    public int metricRegroup(final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        commands.add(MetricRegroup.createMetricRegroup(stat, min, max, intervalSize, noGutters, remoteImhotepMultiSession.getSessionId()));
        return -999;
    }

    @Override
    public int regroup(final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        commands.add(new UnconditionalRegroup(fromGroups, toGroups, filterOutNotTargeted, remoteImhotepMultiSession.getSessionId()));
        return -999;
    }

    @Override
    public int metricFilter(final List<String> stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        commands.add(new UntargetedMetricFilter(stat, min, max, negate, remoteImhotepMultiSession.getSessionId()));
        return -999;
    }

    @Override
    public int metricFilter(final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new TargetedMetricFilter(stat, min, max, targetGroup, negativeGroup, positiveGroup, remoteImhotepMultiSession.getSessionId()));
        return -999;
    }

    @Override
    public List<TermCount> approximateTopTerms(final String field, final boolean isIntField, final int k) {
        return remoteImhotepMultiSession.approximateTopTerms(field, isIntField, k);
    }

    @Override
    public int pushStat(final String statName) throws ImhotepOutOfMemoryException {
        executeBatch();
        return remoteImhotepMultiSession.pushStat(statName);
    }

    @Override
    public int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        executeBatch();
        return remoteImhotepMultiSession.pushStats(statNames);
    }

    @Override
    public int popStat() {
        executeBatchNoMemoryException();
        return remoteImhotepMultiSession.popStat();
    }

    @Override
    public int getNumStats() {
        return remoteImhotepMultiSession.getNumStats();
    }

    @Override
    public int getNumGroups() {
        executeBatchNoMemoryException();
        return remoteImhotepMultiSession.getNumGroups();
    }

    @Override
    public void createDynamicMetric(final String name) throws ImhotepOutOfMemoryException {
        executeBatch();
        remoteImhotepMultiSession.createDynamicMetric(name);
    }

    @Override
    public void updateDynamicMetric(final String name, final int[] deltas) throws ImhotepOutOfMemoryException {
        executeBatch();
        remoteImhotepMultiSession.updateDynamicMetric(name, deltas);
    }

    @Override
    public void conditionalUpdateDynamicMetric(final String name, final RegroupCondition[] conditions, final int[] deltas) {
        executeBatchNoMemoryException();
        remoteImhotepMultiSession.conditionalUpdateDynamicMetric(name, conditions, deltas);
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(final String name, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        executeBatchNoMemoryException();
        remoteImhotepMultiSession.groupConditionalUpdateDynamicMetric(name, groups, conditions, deltas);
    }

    @Override
    public void groupQueryUpdateDynamicMetric(final String name, final int[] groups, final Query[] conditions, final int[] deltas) throws ImhotepOutOfMemoryException {
        executeBatch();
        remoteImhotepMultiSession.groupQueryUpdateDynamicMetric(name, groups, conditions, deltas);
    }

    @Override
    public void close() {
        if (!commands.isEmpty()) {
            log.warn("Requested to close a session with id: " + getSessionId() + " without using the regroup calls: " + getLogCommandClassNameList());
        }
        commands.clear();
        remoteImhotepMultiSession.close();
    }

    @Override
    public void resetGroups() throws ImhotepOutOfMemoryException {
        commands.clear();
        remoteImhotepMultiSession.resetGroups();
    }

    @Override
    public void rebuildAndFilterIndexes(final List<String> intFields, final List<String> stringFields) throws ImhotepOutOfMemoryException {
        executeBatch();
        remoteImhotepMultiSession.rebuildAndFilterIndexes(intFields, stringFields);
    }

    @Override
    public long getNumDocs() {
        return remoteImhotepMultiSession.getNumDocs();
    }

    @Override
    public PerformanceStats getPerformanceStats(final boolean reset) {
        return remoteImhotepMultiSession.getPerformanceStats(reset);
    }

    @Override
    public PerformanceStats closeAndGetPerformanceStats() {
        if (!commands.isEmpty()) {
            log.warn("Requested to close a session with id: " + getSessionId() + " without using the regroup calls: " + getLogCommandClassNameList());
        }
        return remoteImhotepMultiSession.closeAndGetPerformanceStats();
    }

    @Override
    public void addObserver(final Instrumentation.Observer observer) {
        remoteImhotepMultiSession.addObserver(observer);
    }

    @Override
    public void removeObserver(final Instrumentation.Observer observer) {
        remoteImhotepMultiSession.removeObserver(observer);
    }

    public long getTempFilesBytesWritten() {
        executeBatchNoMemoryException();
        return remoteImhotepMultiSession.getTempFilesBytesWritten();
    }

    private List<String> getLogCommandClassNameList() {
        return commands.stream().map(x -> x.getClass().getSimpleName()).collect(Collectors.toList());
    }
}
