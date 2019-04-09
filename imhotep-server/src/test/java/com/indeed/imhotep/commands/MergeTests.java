package com.indeed.imhotep.commands;

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.GroupStatsDummyIterator;
import com.indeed.imhotep.GroupStatsIteratorCombiner;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
        Assert.assertEquals(Collections.max(subResults), imhotepCommand.combine(subResults));
    }

    @Test
    public void testGetGroupStats() {
        final List<String> stats = Lists.newArrayList("1");
        final GetGroupStats getGroupStats = new GetGroupStats(stats, SESSION_ID);
        final List<GroupStatsIterator> groupStatsDummyIterators = new ArrayList<>();
        groupStatsDummyIterators.add(new GroupStatsDummyIterator(new long[]{1, 10, 30, 123}));
        groupStatsDummyIterators.add(new GroupStatsDummyIterator(new long[]{112, 110, 390, 123}));
        Assert.assertEquals(getGroupStats.combine(groupStatsDummyIterators), new GroupStatsIteratorCombiner(groupStatsDummyIterators.toArray(new GroupStatsIterator[0])));
    }

    @Test
    public void testIntOrRegroup() {
        final IntOrRegroup intOrRegroup = new IntOrRegroup( "field", new long[]{1,3,4}, 1, 2, 3, SESSION_ID);
        assertVoidMerge(intOrRegroup);
    }

    @Test
    public void testMetricFilter() {
        final List<String> stats = Lists.newArrayList("1");
        final MetricFilter metricFilter = new MetricFilter(stats, 1, 5, 1, 2, 3, SESSION_ID);
        assertMaxMerge(metricFilter);
    }

    @Test
    public void testNegateMetricFilter() {
        final List<String> stats = Lists.newArrayList("1");
        final NegateMetricFilter negateMetricFilter = new NegateMetricFilter(stats, -5, 5, true, SESSION_ID);
        assertMaxMerge(negateMetricFilter);
    }

    @Test
    public void testMetricRegroup() {
        final List<String> stats = Lists.newArrayList("1");
        final MetricRegroup metricRegroup = MetricRegroup.createMetricRegroup(stats, 1, 5, 3, true, SESSION_ID);
        assertMaxMerge(metricRegroup);
    }

    @Test
    public void testMultiRegroup() {
        final GroupMultiRemapRule[] rawRules = new GroupMultiRemapRule[] {
                new GroupMultiRemapRule(1, 10, new int[]{10}, new RegroupCondition[]{new RegroupCondition("field", false, 2, "string", false)})
        };
        final MultiRegroup multiRegroup = MultiRegroup.createMultiRegroupCommand(rawRules, true , SESSION_ID);
        assertMaxMerge(multiRegroup);

    }

    @Test
    public void testMultiRegroupMessagesSender() {
        final GroupMultiRemapRule[] rules =  new GroupMultiRemapRule[] {
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        final GroupMultiRemapRuleSender groupMultiRemapRuleSender = GroupMultiRemapRuleSender.createFromRules(Arrays.asList(rules).iterator(), true);
        final MultiRegroupMessagesSender multiRegroupMessagesSender = MultiRegroupMessagesSender.createMultiRegroupMessagesSender(groupMultiRemapRuleSender, true, SESSION_ID);
        assertMaxMerge(multiRegroupMessagesSender);
    }

    @Test
    public void testMultiRegroupMessagesIterator() {
        final GroupMultiRemapRule[] rules =  new GroupMultiRemapRule[] {
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        final Iterator<GroupMultiRemapRule> groupMultiRemapRuleIterator = Arrays.asList(rules).iterator();
        final MultiRegroupIterator multiRegroupIterator = new MultiRegroupIterator(1, groupMultiRemapRuleIterator, false, SESSION_ID);
        assertMaxMerge(multiRegroupIterator);
    }

    @Test
    public void testRandomMetricMultiRegroup() {
        final List<String> stats = Lists.newArrayList("1");
        final RandomMetricMultiRegroup randomMetricMultiRegroup = new RandomMetricMultiRegroup(stats, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID);
        assertVoidMerge(randomMetricMultiRegroup);
    }

    @Test
    public void testRandomMetricRegroup() {
        final List<String> stats = Lists.newArrayList("1");
        final RandomMetricRegroup randomMetricRegroup = new RandomMetricRegroup(stats, RANDOM_SALT, 0.4, 1, 2, 3, SESSION_ID);
        assertVoidMerge(randomMetricRegroup);
    }

    @Test
    public void testRandomMultiRegroup() {
        final RandomMultiRegroup randomMultiRegroup = new RandomMultiRegroup("fieldName", false, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID);
        assertVoidMerge(randomMultiRegroup);
    }

    @Test
    public void testRandomRegroup() {
        final RandomRegroup randomRegroup = new RandomRegroup("fieldName", true, RANDOM_SALT, 0.03, 1, 2, 3, SESSION_ID);
        assertVoidMerge(randomRegroup);
    }

    @Test
    public void testRegexRegroup() {
        final RegexRegroup regexRegroup = new RegexRegroup("fieldName", ".*.*", 1, 2, 3, SESSION_ID);
        assertVoidMerge(regexRegroup);
    }

    @Test
    public void testRegroup() {
        final GroupRemapRule[] rawRules = new GroupRemapRule[] {
                new GroupRemapRule(1, new RegroupCondition("fieldName", false, 0, "strTerm", false), 1000000, 1000000)
        };
        final Regroup regroup = Regroup.createRegroup(rawRules, SESSION_ID);
        assertMaxMerge(regroup);
    }

    @Test
    public void testQueryRemapRuleRegroup() {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")),1, 2);
        final QueryRemapRuleRegroup queryRemapRuleRegroup = new QueryRemapRuleRegroup(rule, SESSION_ID);
        assertMaxMerge(queryRemapRuleRegroup);
    }

    @Test
    public void testUnconditionalRegroup() {
        final UnconditionalRegroup unconditionalRegroup = new UnconditionalRegroup(new int[]{1,2,3}, new int[]{12,43,12}, true, SESSION_ID);
        assertMaxMerge(unconditionalRegroup);
    }

    @Test
    public void testStringOrRegroup() {
        final List<String> terms = Lists.newArrayList("1");
        final StringOrRegroup stringOrRegroup = new StringOrRegroup("fieldName", terms, 1, 2, 3, SESSION_ID);
        assertVoidMerge(stringOrRegroup);
    }
}
