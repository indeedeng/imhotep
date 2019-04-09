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
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MergeTests implements CommandsTest {

    public static final String SESSION_ID = "RandomSessionIdString";
    public static final String RANDOM_SALT = "RandomSaltString";

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

    @Test
    public void testGetGroupStats() {
        final List<String> stats = Lists.newArrayList("1");
        final GetGroupStats getGroupStats = new GetGroupStats(stats, SESSION_ID);
        final List<GroupStatsIterator> groupStatsDummyIterators = new ArrayList<>();
        groupStatsDummyIterators.add(new GroupStatsDummyIterator(new long[]{1, 10, 30, 123}));
        groupStatsDummyIterators.add(new GroupStatsDummyIterator(new long[]{112, 110, 390, 123}));
        assertEqualGroupStatsIterators(new GroupStatsDummyIterator(new long[]{113, 120, 420, 246}), getGroupStats.combine(groupStatsDummyIterators));
    }

    @Test
    public void testIntOrRegroup() {
        assertVoidMerge(new IntOrRegroup("field", new long[]{1, 3, 4}, 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testMetricFilter() {
        final List<String> stats = Lists.newArrayList("1");
        assertMaxMerge(new MetricFilter(stats, 1, 5, 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testNegateMetricFilter() {
        final List<String> stats = Lists.newArrayList("1");
        assertMaxMerge(new NegateMetricFilter(stats, -5, 5, true, SESSION_ID));
    }

    @Test
    public void testMetricRegroup() {
        final List<String> stats = Lists.newArrayList("1");
        assertMaxMerge(MetricRegroup.createMetricRegroup(stats, 1, 5, 3, true, SESSION_ID));
    }

    @Test
    public void testMultiRegroup() {
        final GroupMultiRemapRule[] rawRules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 10, new int[]{10}, new RegroupCondition[]{new RegroupCondition("field", false, 2, "string", false)})
        };
        assertMaxMerge(MultiRegroup.createMultiRegroupCommand(rawRules, true, SESSION_ID));

    }

    @Test
    public void testMultiRegroupMessagesSender() {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        final GroupMultiRemapRuleSender groupMultiRemapRuleSender = GroupMultiRemapRuleSender.createFromRules(Arrays.asList(rules).iterator(), true);
        assertMaxMerge(MultiRegroupMessagesSender.createMultiRegroupMessagesSender(groupMultiRemapRuleSender, true, SESSION_ID));
    }

    @Test
    public void testMultiRegroupMessagesIterator() {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        final Iterator<GroupMultiRemapRule> groupMultiRemapRuleIterator = Arrays.asList(rules).iterator();
        assertMaxMerge(new MultiRegroupIterator(1, groupMultiRemapRuleIterator, false, SESSION_ID));
    }

    @Test
    public void testRandomMetricMultiRegroup() {
        final List<String> stats = Lists.newArrayList("1");
        assertVoidMerge(new RandomMetricMultiRegroup(stats, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Test
    public void testRandomMetricRegroup() {
        final List<String> stats = Lists.newArrayList("1");
        assertVoidMerge(new RandomMetricRegroup(stats, RANDOM_SALT, 0.4, 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testRandomMultiRegroup() {
        assertVoidMerge(new RandomMultiRegroup("fieldName", false, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Test
    public void testRandomRegroup() {
        assertVoidMerge(new RandomRegroup("fieldName", true, RANDOM_SALT, 0.03, 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testRegexRegroup() {
        assertVoidMerge(new RegexRegroup("fieldName", ".*.*", 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testRegroup() {
        final GroupRemapRule[] rawRules = new GroupRemapRule[]{
                new GroupRemapRule(1, new RegroupCondition("fieldName", false, 0, "strTerm", false), 1000000, 1000000)
        };
        assertMaxMerge(Regroup.createRegroup(rawRules, SESSION_ID));
    }

    @Test
    public void testQueryRegroup() {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")), 1, 2);
        assertMaxMerge(new QueryRegroup(rule, SESSION_ID));
    }

    @Test
    public void testUnconditionalRegroup() {
        assertMaxMerge(new UnconditionalRegroup(new int[]{1, 2, 3}, new int[]{12, 43, 12}, true, SESSION_ID));
    }

    @Test
    public void testStringOrRegroup() {
        final List<String> terms = Lists.newArrayList("1");
        assertVoidMerge(new StringOrRegroup("fieldName", terms, 1, 2, 3, SESSION_ID));
    }
}
