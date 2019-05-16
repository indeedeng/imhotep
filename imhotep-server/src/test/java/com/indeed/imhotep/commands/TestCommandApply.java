package com.indeed.imhotep.commands;

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.GroupStatsDummyIterator;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import com.indeed.imhotep.protobuf.Operator;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestCommandApply implements CommandsTest {

    public static final String SESSION_ID = "RandomSessionIdString";
    public static final String RANDOM_SALT = "RandomSaltString";

    private ImhotepSession imhotepSession;

    private interface ThrowingFunction<K, V> {
        V apply(K k) throws ImhotepOutOfMemoryException;
    }

    public interface VoidThrowingFunction<K> {
        void apply(K k) throws ImhotepOutOfMemoryException;
    }

    @Before
    public void setup() {
        imhotepSession = EasyMock.createMock(ImhotepSession.class);
    }

    private void replayAndVerify(final ImhotepCommand command) throws ImhotepOutOfMemoryException {
        EasyMock.replay(imhotepSession);
        command.apply(imhotepSession);
        EasyMock.verify(imhotepSession);
    }

    private void testApplyMethodCallInt(final ImhotepCommand<Integer> command, final ThrowingFunction<ImhotepSession, Integer> sessionApplyFunction) throws ImhotepOutOfMemoryException {
        EasyMock.expect(sessionApplyFunction.apply(imhotepSession)).andReturn(1);
        replayAndVerify(command);
    }

    private void testApplyMethodCallVoid(final ImhotepCommand command, final VoidThrowingFunction<ImhotepSession> sessionApplyFunction) throws ImhotepOutOfMemoryException {
        sessionApplyFunction.apply(imhotepSession);
        replayAndVerify(command);
    }

    @Override
    @Test
    public void testGetGroupStats() throws Exception {
        final List<String> stats = Lists.newArrayList("1");
        EasyMock.expect(imhotepSession.getGroupStatsIterator(ImhotepSession.DEFAULT_GROUPS, stats)).andReturn(new GroupStatsDummyIterator(new long[]{1, 2, 3}));
        EasyMock.replay(imhotepSession);

        final GetGroupStats getGroupStats = new GetGroupStats(ImhotepSession.DEFAULT_GROUPS, stats, SESSION_ID);
        getGroupStats.apply(imhotepSession);
        EasyMock.verify(imhotepSession);
    }

    @Override
    @Test
    public void testIntOrRegroup() throws ImhotepOutOfMemoryException {
        testApplyMethodCallVoid(new IntOrRegroup(RegroupParams.DEFAULT, "field", new long[]{1, 3, 4}, 1, 2, 3, SESSION_ID), imhotepSession -> {
            imhotepSession.intOrRegroup(EasyMock.eq(RegroupParams.DEFAULT), EasyMock.eq("field"), EasyMock.aryEq(new long[]{1, 3, 4}), EasyMock.eq(1), EasyMock.eq(2), EasyMock.eq(3));
        });
    }

    @Override
    @Test
    public void testTargetedMetricFilter() throws ImhotepOutOfMemoryException {
        final List<String> stat = Lists.newArrayList("1");
        testApplyMethodCallInt(new TargetedMetricFilter(RegroupParams.DEFAULT, stat, 0, 100, 1, 2, 3, SESSION_ID), imhotepSession -> {
            return imhotepSession.metricFilter(RegroupParams.DEFAULT, stat, 0L, 100L, 1, 2, 3);
        });
    }

    @Override
    @Test
    public void testMetricRegroup() throws ImhotepOutOfMemoryException {
        final List<String> stats = Lists.newArrayList("1");
        testApplyMethodCallInt(MetricRegroup.createMetricRegroup(RegroupParams.DEFAULT, stats, 0, 100, 10, false, SESSION_ID), imhotepSession -> {
            return imhotepSession.metricRegroup(RegroupParams.DEFAULT, stats, 0L, 100L, 10L, false);
        });
    }

    @Override
    @Test
    public void testMultiRegroup() throws ImhotepOutOfMemoryException {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{new RegroupCondition("metric", true, 3, null, false)})};
        testApplyMethodCallInt(MultiRegroup.createMultiRegroupCommand(RegroupParams.DEFAULT, rules, true, SESSION_ID), imhotepSession -> {
            return imhotepSession.regroup(RegroupParams.DEFAULT, rules, true);
        });
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testMultiRegroupMessagesSender() throws ImhotepOutOfMemoryException {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        final GroupMultiRemapRuleSender groupMultiRemapRuleSender = GroupMultiRemapRuleSender.createFromRules(Arrays.asList(rules).iterator(), true);
        testApplyMethodCallInt(MultiRegroupMessagesSender.createMultiRegroupMessagesSender(RegroupParams.DEFAULT, groupMultiRemapRuleSender, false, SESSION_ID), imhotepSession -> {
            return imhotepSession.regroup(RegroupParams.DEFAULT, rules, false);
        });
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testMultiRegroupMessagesIterator() throws ImhotepOutOfMemoryException {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        testApplyMethodCallInt(new MultiRegroupIterator(RegroupParams.DEFAULT, 1, Arrays.asList(rules).iterator(), false, SESSION_ID), imhotepSession -> {
            return imhotepSession.regroup(RegroupParams.DEFAULT, rules, false);
        });
    }

    @Override
    @Test
    public void testUntargetedMetricFilter() throws ImhotepOutOfMemoryException {
        final List<String> stats = Lists.newArrayList("1");
        testApplyMethodCallInt(new UntargetedMetricFilter(RegroupParams.DEFAULT, stats, 0, 5, true, SESSION_ID), imhotepSession -> {
            return imhotepSession.metricFilter(RegroupParams.DEFAULT, stats, 0L, 5L, true);
        });
    }

    @Override
    @Test
    public void testRandomMetricMultiRegroup() throws ImhotepOutOfMemoryException {
        final List<String> stats = Lists.newArrayList("1");
        testApplyMethodCallVoid(new RandomMetricMultiRegroup(RegroupParams.DEFAULT, stats, "RandomSaltString", 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID), imhotepSession -> {
            imhotepSession.randomMetricMultiRegroup(EasyMock.eq(RegroupParams.DEFAULT), EasyMock.eq(stats), EasyMock.eq(RANDOM_SALT), EasyMock.eq(1), EasyMock.aryEq(new double[]{0.4, 0.8}), EasyMock.aryEq(new int[]{3, 4, 6}));
        });
    }

    @Override
    @Test
    public void testRandomMetricRegroup() throws ImhotepOutOfMemoryException {
        final List<String> stats = Lists.newArrayList("1");
        testApplyMethodCallVoid(new RandomMetricRegroup(RegroupParams.DEFAULT, stats, RANDOM_SALT, 0.3, 1, 2, 3, SESSION_ID), imhotepSession -> {
            imhotepSession.randomMetricRegroup(RegroupParams.DEFAULT, stats, RANDOM_SALT, 0.3, 1, 2, 3);
        });
    }

    @Override
    @Test
    public void testRandomMultiRegroup() throws ImhotepOutOfMemoryException {
        testApplyMethodCallVoid(new RandomMultiRegroup(RegroupParams.DEFAULT, "field", true, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID), imhotepSession -> {
            imhotepSession.randomMultiRegroup(EasyMock.eq(RegroupParams.DEFAULT), EasyMock.eq("field"), EasyMock.eq(true), EasyMock.eq(RANDOM_SALT), EasyMock.eq(1), EasyMock.aryEq(new double[]{0.4, 0.8}), EasyMock.aryEq(new int[]{3, 4, 6}));
        });
    }

    @Override
    @Test
    public void testRandomRegroup() throws ImhotepOutOfMemoryException {
        testApplyMethodCallVoid(new RandomRegroup(RegroupParams.DEFAULT, "field", true, RANDOM_SALT, 0.2, 1, 2, 3, SESSION_ID), imhotepSession -> {
            imhotepSession.randomRegroup(RegroupParams.DEFAULT, "field", true, RANDOM_SALT, 0.2, 1, 2, 3);
        });
    }

    @Override
    @Test
    public void testRegexRegroup() throws ImhotepOutOfMemoryException {
        testApplyMethodCallVoid(new RegexRegroup(RegroupParams.DEFAULT, "field", ".*.*", 1, 2, 3, SESSION_ID), imhotepSession -> {
            imhotepSession.regexRegroup(RegroupParams.DEFAULT, "field", ".*.*", 1, 2, 3);
        });
    }

    @Override
    @Test
    public void testRegroup() throws ImhotepOutOfMemoryException {
        final GroupRemapRule[] rawRules = new GroupRemapRule[]{
                new GroupRemapRule(1, new RegroupCondition("fieldName", false, 0, "strTerm", false), 1000000, 1000000)
        };
        testApplyMethodCallInt(Regroup.createRegroup(RegroupParams.DEFAULT, rawRules, SESSION_ID), imhotepSession -> {
            return imhotepSession.regroup(EasyMock.eq(RegroupParams.DEFAULT), EasyMock.aryEq(rawRules));
        });
    }

    @Override
    @Test
    public void testQueryRegroup() throws ImhotepOutOfMemoryException {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")), 1, 2);
        testApplyMethodCallInt(new QueryRegroup(RegroupParams.DEFAULT, rule, SESSION_ID), imhotepSession -> {
            return imhotepSession.regroup(RegroupParams.DEFAULT, rule);
        });
    }

    @Override
    @Test
    public void testUnconditionalRegroup() throws ImhotepOutOfMemoryException {
        testApplyMethodCallInt(new UnconditionalRegroup(RegroupParams.DEFAULT, new int[]{1, 2, 3}, new int[]{12, 43, 12}, true, SESSION_ID), imhotepSession -> {
            return imhotepSession.regroup(EasyMock.eq(RegroupParams.DEFAULT), EasyMock.aryEq(new int[]{1, 2, 3}), EasyMock.aryEq(new int[]{12, 43, 12}), EasyMock.eq(true));
        });
    }

    @Override
    @Test
    public void testStringOrRegroup() throws ImhotepOutOfMemoryException {
        final List<String> terms = Lists.newArrayList("1");
        testApplyMethodCallVoid(new StringOrRegroup(RegroupParams.DEFAULT, "field", terms, 1, 2, 3, SESSION_ID), imhotepSession -> {
            imhotepSession.stringOrRegroup(EasyMock.eq(RegroupParams.DEFAULT), EasyMock.eq("field"), EasyMock.aryEq(terms.toArray(new String[0])), EasyMock.eq(1), EasyMock.eq(2), EasyMock.eq(3));
        });
    }

    @Override
    @Test
    public void testConsolidateGroups() throws Exception {
        final ArrayList<String> inputGroups = Lists.newArrayList("groups1", "groups2");
        testApplyMethodCallVoid(new ConsolidateGroups(inputGroups, Operator.AND, "outputGroups", SESSION_ID), session -> {
            session.consolidateGroups(inputGroups, Operator.AND, "outputGroups");
        });
    }

    @Override
    @Test
    public void testResetGroups() throws Exception {
        testApplyMethodCallVoid(new ResetGroups("foo", SESSION_ID), session -> {
            session.resetGroups("foo");
        });
    }

    @Override
    @Test
    public void testDeleteGroups() throws Exception {
        final List<String> groupsToDelete = Collections.singletonList("someGroups");
        testApplyMethodCallVoid(new DeleteGroups(groupsToDelete, SESSION_ID), session -> {
            session.deleteGroups(groupsToDelete);
        });
    }
}
