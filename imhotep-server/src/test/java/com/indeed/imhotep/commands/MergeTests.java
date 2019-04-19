package com.indeed.imhotep.commands;

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.GroupStatsDummyIterator;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import com.indeed.imhotep.protobuf.Operator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MergeTests implements CommandsTest {

    private static final String SESSION_ID = "RandomSessionIdString";
    private static final String RANDOM_SALT = "RandomSaltString";
    private static final String TEST_INPUT_GROUPS_NAME = "myOtherTestGroups";
    private static final RegroupParams TEST_REGROUP_PARAMS = new RegroupParams("myInputGroups", "myOutputGroups");


    public void assertVoidMerge(final ImhotepCommand imhotepCommand) {
        final List<Void> subResults = new ArrayList<>();
        subResults.add(null);
        Assert.assertEquals(imhotepCommand.combine(subResults), null);
    }

    public void assertMaxMerge(final ImhotepCommand imhotepCommand) {
        final List<Integer> subResults = Lists.newArrayList(100, 1, 350, 1, 5, 10);
        Assert.assertEquals(350, imhotepCommand.combine(subResults));
    }

    private void assertEqualGroupStatsIterators(final GroupStatsIterator iterator1, final GroupStatsIterator iterator2) {
        while (iterator1.hasNext() || iterator2.hasNext()) {
            if (iterator1.hasNext() != iterator2.hasNext()) {
                throw new AssertionError("GroupStatsIterators of unequal size.");
            }
            Assert.assertEquals(iterator1.next(), iterator2.next());
        }
    }

    @Override
    @Test
    public void testGetGroupStats() {
        final List<String> stats = Lists.newArrayList("1");
        final GetGroupStats getGroupStats = new GetGroupStats(TEST_INPUT_GROUPS_NAME, stats, SESSION_ID);
        final List<GroupStatsIterator> groupStatsDummyIterators = new ArrayList<>();
        groupStatsDummyIterators.add(new GroupStatsDummyIterator(new long[]{1, 10, 30, 123}));
        groupStatsDummyIterators.add(new GroupStatsDummyIterator(new long[]{112, 110, 390, 123}));
        assertEqualGroupStatsIterators(new GroupStatsDummyIterator(new long[]{113, 120, 420, 246}), getGroupStats.combine(groupStatsDummyIterators));
    }

    @Override
    @Test
    public void testIntOrRegroup() {
        assertVoidMerge(new IntOrRegroup(TEST_REGROUP_PARAMS, "field", new long[]{1, 3, 4}, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testTargetedMetricFilter() {
        final List<String> stats = Lists.newArrayList("1");
        assertMaxMerge(new TargetedMetricFilter(TEST_REGROUP_PARAMS, stats, 1, 5, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testUntargetedMetricFilter() {
        final List<String> stats = Lists.newArrayList("1");
        assertMaxMerge(new UntargetedMetricFilter(TEST_REGROUP_PARAMS, stats, -5, 5, true, SESSION_ID));
    }

    @Override
    @Test
    public void testMetricRegroup() {
        final List<String> stats = Lists.newArrayList("1");
        assertMaxMerge(MetricRegroup.createMetricRegroup(TEST_REGROUP_PARAMS, stats, 1, 5, 3, true, SESSION_ID));
    }

    @Override
    @Test
    public void testMultiRegroup() {
        final GroupMultiRemapRule[] rawRules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 10, new int[]{10}, new RegroupCondition[]{new RegroupCondition("field", false, 2, "string", false)})
        };
        assertMaxMerge(MultiRegroup.createMultiRegroupCommand(TEST_REGROUP_PARAMS, rawRules, true, SESSION_ID));

    }

    @Override
    @Test
    public void testMultiRegroupMessagesSender() {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        final GroupMultiRemapRuleSender groupMultiRemapRuleSender = GroupMultiRemapRuleSender.createFromRules(Arrays.asList(rules).iterator(), true);
        assertMaxMerge(MultiRegroupMessagesSender.createMultiRegroupMessagesSender(TEST_REGROUP_PARAMS, groupMultiRemapRuleSender, true, SESSION_ID));
    }

    @Override
    @Test
    public void testMultiRegroupMessagesIterator() {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        final Iterator<GroupMultiRemapRule> groupMultiRemapRuleIterator = Arrays.asList(rules).iterator();
        assertMaxMerge(new MultiRegroupIterator(TEST_REGROUP_PARAMS, 1, groupMultiRemapRuleIterator, false, SESSION_ID));
    }

    @Override
    @Test
    public void testRandomMetricMultiRegroup() {
        final List<String> stats = Lists.newArrayList("1");
        assertVoidMerge(new RandomMetricMultiRegroup(TEST_REGROUP_PARAMS, stats, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Override
    @Test
    public void testRandomMetricRegroup() {
        final List<String> stats = Lists.newArrayList("1");
        assertVoidMerge(new RandomMetricRegroup(TEST_REGROUP_PARAMS, stats, RANDOM_SALT, 0.4, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testRandomMultiRegroup() {
        assertVoidMerge(new RandomMultiRegroup(TEST_REGROUP_PARAMS, "fieldName", false, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Override
    @Test
    public void testRandomRegroup() {
        assertVoidMerge(new RandomRegroup(TEST_REGROUP_PARAMS, "fieldName", true, RANDOM_SALT, 0.03, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testRegexRegroup() {
        assertVoidMerge(new RegexRegroup(TEST_REGROUP_PARAMS, "fieldName", ".*.*", 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testRegroup() {
        final GroupRemapRule[] rawRules = new GroupRemapRule[]{
                new GroupRemapRule(1, new RegroupCondition("fieldName", false, 0, "strTerm", false), 1000000, 1000000)
        };
        assertMaxMerge(Regroup.createRegroup(TEST_REGROUP_PARAMS, rawRules, SESSION_ID));
    }

    @Override
    @Test
    public void testQueryRegroup() {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")), 1, 2);
        assertMaxMerge(new QueryRegroup(TEST_REGROUP_PARAMS, rule, SESSION_ID));
    }

    @Override
    @Test
    public void testUnconditionalRegroup() {
        assertMaxMerge(new UnconditionalRegroup(TEST_REGROUP_PARAMS, new int[]{1, 2, 3}, new int[]{12, 43, 12}, true, SESSION_ID));
    }

    @Override
    @Test
    public void testStringOrRegroup() {
        final List<String> terms = Lists.newArrayList("1");
        assertVoidMerge(new StringOrRegroup(TEST_REGROUP_PARAMS, "fieldName", terms, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testConsolidateGroups() throws Exception {
        assertVoidMerge(new ConsolidateGroups(Collections.singletonList(ImhotepSession.DEFAULT_GROUPS), Operator.NOT, ImhotepSession.DEFAULT_GROUPS, SESSION_ID));
    }
}
