package com.indeed.imhotep;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.commands.ConsolidateGroups;
import com.indeed.imhotep.commands.DeleteGroups;
import com.indeed.imhotep.commands.GetGroupStats;
import com.indeed.imhotep.commands.IntOrRegroup;
import com.indeed.imhotep.commands.MetricRegroup;
import com.indeed.imhotep.commands.MultiRegroup;
import com.indeed.imhotep.commands.MultiRegroupIterator;
import com.indeed.imhotep.commands.MultiRegroupMessagesSender;
import com.indeed.imhotep.commands.OpenSessions;
import com.indeed.imhotep.commands.QueryRegroup;
import com.indeed.imhotep.commands.RandomMetricMultiRegroup;
import com.indeed.imhotep.commands.RandomMetricRegroup;
import com.indeed.imhotep.commands.RandomMultiRegroup;
import com.indeed.imhotep.commands.RandomRegroup;
import com.indeed.imhotep.commands.RegexRegroup;
import com.indeed.imhotep.commands.ResetGroups;
import com.indeed.imhotep.commands.StringOrRegroup;
import com.indeed.imhotep.commands.TargetedMetricFilter;
import com.indeed.imhotep.commands.UnconditionalRegroup;
import com.indeed.imhotep.commands.UntargetedMetricFilter;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.Operator;
import it.unimi.dsi.fastutil.longs.LongIterators;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    public BatchRemoteImhotepMultiSession(final OpenSessions openSessions) {
        super(openSessions.getSessionId());
        remoteImhotepMultiSession = openSessions.makeSession();
        commands.add(openSessions);
    }

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

    public void executeBatchNoMemoryException() {
        try {
            executeBatch();
        } catch (final ImhotepOutOfMemoryException e) {
            log.error("ImhotepOutOfMemoryException while executing Imhotep Batch commands. SessionId: " + getSessionId() + " commands class List: " + getLogCommandClassNameList());
            throw Throwables.propagate(e);
        }
    }

    @Override
    public long getTotalDocFreq(final String[] intFields, final String[] stringFields) {
        return remoteImhotepMultiSession.getTotalDocFreq(intFields, stringFields);
    }

    @Override
    public long[] getGroupStats(final String groupsName, final List<String> stat) throws ImhotepOutOfMemoryException {
        try (final GroupStatsIterator groupStatsIterator = executeBatchAndGetResult(new GetGroupStats(groupsName, stat, getSessionId()))) {
            return LongIterators.unwrap(groupStatsIterator, groupStatsIterator.getNumGroups());
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final String groupsName, final List<String> stat) throws ImhotepOutOfMemoryException {
        return executeBatchAndGetResult(new GetGroupStats(groupsName, stat, getSessionId()));
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final String groupsName, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        executeBatch();
        return remoteImhotepMultiSession.getSubsetFTGSIterator(groupsName, intFields, stringFields, stats);
    }

    @Override
    public FTGSIterator getFTGSIterator(final String groupsName, final FTGSParams params) throws ImhotepOutOfMemoryException {
        executeBatch();
        return remoteImhotepMultiSession.getFTGSIterator(groupsName, params);
    }

    @Override
    public GroupStatsIterator getDistinct(final String groupsName, final String field, final boolean isIntField) {
        executeBatchNoMemoryException();
        return remoteImhotepMultiSession.getDistinct(groupsName, field, isIntField);
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        commands.add(MultiRegroup.createMultiRegroupCommand(regroupParams, rawRules, errorOnCollisions, getSessionId()));
        return -999;
    }

    @Override
    public int regroupWithProtos(final RegroupParams regroupParams, final GroupMultiRemapMessage[] rawRuleMessages, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        commands.add(MultiRegroupMessagesSender.createMultiRegroupMessagesSender(regroupParams, rawRuleMessages, errorOnCollisions, getSessionId()));
        return -999;
    }

    public int regroupWithRuleSender(final RegroupParams regroupParams, final RequestTools.GroupMultiRemapRuleSender ruleSender, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        commands.add(MultiRegroupMessagesSender.createMultiRegroupMessagesSender(regroupParams, ruleSender, errorOnCollisions, getSessionId()));
        return -999;
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        commands.add(new MultiRegroupIterator(regroupParams, numRawRules, rawRules, errorOnCollisions, getSessionId()));
        return -999;
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        commands.add(new QueryRegroup(regroupParams, rule, getSessionId()));
        return -999;
    }

    @Override
    public void intOrRegroup(final RegroupParams regroupParams, final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new IntOrRegroup(regroupParams, field, terms, targetGroup, negativeGroup, positiveGroup, getSessionId()));
    }

    @Override
    public void stringOrRegroup(final RegroupParams regroupParams, final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new StringOrRegroup(regroupParams, field, Arrays.asList(terms), targetGroup, negativeGroup, positiveGroup, getSessionId()));
    }

    @Override
    public void regexRegroup(final RegroupParams regroupParams, final String field, final String regex, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new RegexRegroup(regroupParams, field, regex, targetGroup, negativeGroup, positiveGroup, getSessionId()));
    }

    @Override
    public void randomRegroup(final RegroupParams regroupParams, final String field, final boolean isIntField, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new RandomRegroup(regroupParams, field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup, getSessionId()));
    }

    @Override
    public void randomMultiRegroup(final RegroupParams regroupParams, final String field, final boolean isIntField, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        commands.add(new RandomMultiRegroup(regroupParams, field, isIntField, salt, targetGroup, percentages, resultGroups, getSessionId()));
    }

    @Override
    public void randomMetricRegroup(final RegroupParams regroupParams, final List<String> stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new RandomMetricRegroup(regroupParams, stat, salt, p, targetGroup, negativeGroup, positiveGroup, getSessionId()));
    }

    @Override
    public void randomMetricMultiRegroup(final RegroupParams regroupParams, final List<String> stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        commands.add(new RandomMetricMultiRegroup(regroupParams, stat, salt, targetGroup, percentages, resultGroups, getSessionId()));
    }

    @Override
    public int metricRegroup(final RegroupParams regroupParams, final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        commands.add(MetricRegroup.createMetricRegroup(regroupParams, stat, min, max, intervalSize, noGutters, getSessionId()));
        return -999;
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        commands.add(new UnconditionalRegroup(regroupParams, fromGroups, toGroups, filterOutNotTargeted, getSessionId()));
        return -999;
    }

    @Override
    public int metricFilter(final RegroupParams regroupParams, final List<String> stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        commands.add(new UntargetedMetricFilter(regroupParams, stat, min, max, negate, getSessionId()));
        return -999;
    }

    @Override
    public int metricFilter(final RegroupParams regroupParams, final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        commands.add(new TargetedMetricFilter(regroupParams, stat, min, max, targetGroup, negativeGroup, positiveGroup, getSessionId()));
        return -999;
    }

    @Override
    public List<TermCount> approximateTopTerms(final String field, final boolean isIntField, final int k) {
        return remoteImhotepMultiSession.approximateTopTerms(field, isIntField, k);
    }

    @Override
    public void consolidateGroups(final List<String> inputGroups, final Operator operation, final String outputGroups) throws ImhotepOutOfMemoryException {
        commands.add(new ConsolidateGroups(inputGroups, operation, outputGroups, getSessionId()));
    }

    @Override
    public void deleteGroups(final List<String> groupsToDelete) {
        commands.add(new DeleteGroups(groupsToDelete, getSessionId()));
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
    public int getNumGroups(final String groupsName) {
        executeBatchNoMemoryException();
        return remoteImhotepMultiSession.getNumGroups(groupsName);
    }

    @Override
    public void createDynamicMetric(final String name) throws ImhotepOutOfMemoryException {
        executeBatch();
        remoteImhotepMultiSession.createDynamicMetric(name);
    }

    @Override
    public void updateDynamicMetric(final String groupsName, final String name, final int[] deltas) throws ImhotepOutOfMemoryException {
        executeBatch();
        remoteImhotepMultiSession.updateDynamicMetric(groupsName, name, deltas);
    }

    @Override
    public void conditionalUpdateDynamicMetric(final String name, final RegroupCondition[] conditions, final int[] deltas) {
        executeBatchNoMemoryException();
        remoteImhotepMultiSession.conditionalUpdateDynamicMetric(name, conditions, deltas);
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(final String groupsName, final String name, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        executeBatchNoMemoryException();
        remoteImhotepMultiSession.groupConditionalUpdateDynamicMetric(groupsName, name, groups, conditions, deltas);
    }

    @Override
    public void groupQueryUpdateDynamicMetric(final String groupsName, final String name, final int[] groups, final Query[] conditions, final int[] deltas) throws ImhotepOutOfMemoryException {
        executeBatch();
        remoteImhotepMultiSession.groupQueryUpdateDynamicMetric(groupsName, name, groups, conditions, deltas);
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
    public void resetGroups(final String groupsName) {
        commands.add(new ResetGroups(groupsName, getSessionId()));
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

    static String getCommandClassName(final ImhotepCommand imhotepCommand) {
        return imhotepCommand.getClass().getSimpleName();
    }

    private List<String> getLogCommandClassNameList() {
        return commands.stream().map(BatchRemoteImhotepMultiSession::getCommandClassName).collect(Collectors.toList());
    }
}
