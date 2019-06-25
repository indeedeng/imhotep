package com.indeed.imhotep.commands;

import com.google.common.collect.Lists;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.BatchRemoteImhotepMultiSession;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import com.indeed.imhotep.protobuf.Operator;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TestImhotepCommands implements CommandsTest {

    private static final String DATASET = "dataset";
    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();

    static Path tempShardDir;
    static Path tempDir;

    private static ShardMasterAndImhotepDaemonClusterRunner clusterRunner;
    private static final MemoryFlamdex memoryFlamdex1 = setFlamdex1();
    private static final MemoryFlamdex memoryFlamdex2 = setFlamdex2();

    private static RemoteImhotepMultiSession imhotepMultiSession;
    private static BatchRemoteImhotepMultiSession batchRemoteImhotepMultiSession;

    private static MemoryFlamdex setFlamdex1() {
        final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();

        memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                .addIntTerm("metric", 1)
                .addIntTerm("metric2", -1)
                .addIntTerms("if1", 1, 2)
                .addIntTerms("if2", 0)
                .addStringTerms("sf1", "1a")
                .addStringTerms("sf2", "a")
                .build());

        memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                .addIntTerm("metric", 2)
                .addIntTerm("metric2", -2)
                .addIntTerms("if1", 21, 22)
                .addIntTerms("if2", 0)
                .addStringTerms("sf1", "2a")
                .addStringTerms("sf2", "a")
                .build());

        memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                .addIntTerm("metric", 3)
                .addIntTerm("metric2", -3)
                .addIntTerms("if1", 31, 32)
                .addIntTerms("if2", 0)
                .addStringTerms("sf1", "3a")
                .addStringTerms("sf2", "a")
                .build());

        return memoryFlamdex;
    }

    private static MemoryFlamdex setFlamdex2() {
        final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();

        memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                .addIntTerm("metric", 4)
                .addIntTerm("metric2", -4)
                .addIntTerms("if1", 41, 42)
                .addIntTerms("if2", 0)
                .addStringTerms("sf1", "4a")
                .addStringTerms("sf2", "a")
                .build());

        memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                .addIntTerm("metric", 5)
                .addIntTerm("metric2", -5)
                .addIntTerms("if1", 51, 52)
                .addIntTerms("if2", 0)
                .addStringTerms("sf1", "5a")
                .addStringTerms("sf2", "a")
                .build());

        memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                .addIntTerm("metric", 6)
                .addIntTerm("metric2", -6)
                .addIntTerms("if1", 61, 62)
                .addIntTerms("if2", 0)
                .addStringTerms("sf1", "6a")
                .addStringTerms("sf2", "a")
                .build());
        return memoryFlamdex;

    }

    @BeforeClass
    public static void imhotepClusterSetup() throws IOException, TimeoutException, InterruptedException {
        tempShardDir = Files.createTempDirectory("shardDir");
        tempDir = Files.createTempDirectory("tempDir");
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(tempShardDir, tempDir);
        clusterRunner.createDailyShard(DATASET, TODAY.minusDays(1), memoryFlamdex1);
        clusterRunner.createDailyShard(DATASET, TODAY.minusDays(2), memoryFlamdex2);
        clusterRunner.startDaemon();
        clusterRunner.startDaemon();
    }

    @Before
    public void setUpMultiSessions() throws InterruptedException, IOException, TimeoutException {
        final ImhotepClient imhotepClient = clusterRunner.createClient();
        imhotepMultiSession = (RemoteImhotepMultiSession) imhotepClient.sessionBuilder(DATASET, TODAY.minusDays(2), TODAY).build();
        batchRemoteImhotepMultiSession = (BatchRemoteImhotepMultiSession) imhotepClient.sessionBuilder(DATASET, TODAY.minusDays(2), TODAY).useBatch(true).build();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        clusterRunner.stop();
        FileUtils.deleteDirectory(tempShardDir.toFile());
        FileUtils.deleteDirectory(tempDir.toFile());
    }

    private interface ThrowingFunction<K, V> {
        V apply(K k) throws ImhotepOutOfMemoryException;
    }

    private interface VoidThrowingFunction<K> {
        void apply(K k) throws ImhotepOutOfMemoryException;
    }

    private void assertEqualGroupStatsInternal() throws ImhotepOutOfMemoryException {
        final List<String> stats = new ArrayList<>();
        stats.add("1");
        Assert.assertArrayEquals(imhotepMultiSession.getGroupStats(stats), batchRemoteImhotepMultiSession.getGroupStats(stats));
    }

    private  <T> void assertEqualGroupStats(final ThrowingFunction<ImhotepSession, T> sessionApplyFunction) throws ImhotepOutOfMemoryException {
        sessionApplyFunction.apply(imhotepMultiSession);
        sessionApplyFunction.apply(batchRemoteImhotepMultiSession);
        assertEqualGroupStatsInternal();
    }

    private void assertEqualGroupStatsVoid(final VoidThrowingFunction<ImhotepSession> sessionApplyFunction) throws ImhotepOutOfMemoryException {
        sessionApplyFunction.apply(imhotepMultiSession);
        sessionApplyFunction.apply(batchRemoteImhotepMultiSession);
        assertEqualGroupStatsInternal();
    }

    @Override
    @Test
    public void testGetGroupStats() throws ImhotepOutOfMemoryException {
        assertEqualGroupStatsInternal();
    }

    @Override
    @Test
    public void testIntOrRegroup() throws ImhotepOutOfMemoryException {
        assertEqualGroupStatsVoid(imhotepSession -> {
            imhotepSession.intOrRegroup("metric", new long[]{1, 4, 5}, 1, 2, 3);
        });
    }

    @Override
    @Test
    public void testTargetedMetricFilter() throws ImhotepOutOfMemoryException {
        final List<String> stat = new ArrayList<String>();
        stat.add("1");
        assertEqualGroupStats(imhotepSession -> {
            return imhotepSession.metricFilter(stat, -5, 5, 1, 2, 3);
        });
    }

    @Override
    @Test
    public void testMetricRegroup() throws ImhotepOutOfMemoryException {
        final List<String> stat = new ArrayList<String>();
        stat.add("1");
        assertEqualGroupStats(imhotepSession -> {
            return imhotepSession.metricRegroup(stat, -5, 5, 1);
        });
    }

    @Override
    @Test
    public void testMultiRegroup() throws ImhotepOutOfMemoryException {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{new RegroupCondition("metric", true, 3, null, false)})};
        assertEqualGroupStats(imhotepSession -> {
            return imhotepSession.regroup(rules, true);
        });
    }

    @Override
    @Test
    public void testMultiRegroupMessagesSender() throws ImhotepOutOfMemoryException {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        final GroupMultiRemapRuleSender groupMultiRemapRuleSender = GroupMultiRemapRuleSender.createFromRules(Arrays.asList(rules).iterator(), true);
        imhotepMultiSession.regroupWithRuleSender(RegroupParams.DEFAULT, groupMultiRemapRuleSender, true);
        batchRemoteImhotepMultiSession.regroupWithRuleSender(RegroupParams.DEFAULT, groupMultiRemapRuleSender, true);
        assertEqualGroupStatsInternal();
    }

    @Override
    @Test
    public void testMultiRegroupMessagesIterator() throws ImhotepOutOfMemoryException {
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                        new RegroupCondition("metric", true, 3, null, false), new RegroupCondition("if2", true, 50, null, false)}),
        };
        Iterator<GroupMultiRemapRule> groupMultiRemapRuleIterator = Arrays.asList(rules).iterator();
        imhotepMultiSession.regroup(1, groupMultiRemapRuleIterator, false);

        groupMultiRemapRuleIterator = Arrays.asList(rules).iterator();
        batchRemoteImhotepMultiSession.regroup(1, groupMultiRemapRuleIterator, false);
        assertEqualGroupStatsInternal();
    }

    @Override
    @Test
    public void testUntargetedMetricFilter() throws ImhotepOutOfMemoryException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualGroupStats(imhotepSession -> {
            return imhotepSession.metricFilter(stats, -5, 5, true);
        });
    }

    @Override
    @Test
    public void testRandomMetricMultiRegroup() throws ImhotepOutOfMemoryException {
        final List<String> stat = new ArrayList<String>();
        stat.add("if2");
        assertEqualGroupStatsInternal();
        assertEqualGroupStatsVoid(imhotepSession -> {
            imhotepSession.randomMetricMultiRegroup(stat, "salt", 1, new double[]{0.2, 0.4}, new int[]{2, 3, 14});
        });
    }

    @Override
    @Test
    public void testRandomMetricRegroup() throws ImhotepOutOfMemoryException {
        final List<String> stat = new ArrayList<String>();
        stat.add("if1");
        assertEqualGroupStatsVoid(imhotepSession -> {
            imhotepSession.randomMetricRegroup(stat, "salt", 0.56, 1, 2, 3);
        });
    }

    @Override
    @Test
    public void testRandomMultiRegroup() throws ImhotepOutOfMemoryException {
        assertEqualGroupStatsVoid(imhotepSession -> {
            imhotepSession.randomMultiRegroup("if2", true, "salt", 1, new double[]{0.4, 0.8}, new int[]{3, 5, 6});
        });
    }

    @Override
    @Test
    public void testRandomRegroup() throws ImhotepOutOfMemoryException {
        assertEqualGroupStatsVoid(imhotepSession -> {
            imhotepSession.randomRegroup("if1", true, "salt123", 0.60, 1, 2, 3);
        });
    }

    @Override
    @Test
    public void testRegexRegroup() throws ImhotepOutOfMemoryException {
        assertEqualGroupStatsVoid(imhotepSession -> {
            imhotepSession.regexRegroup("sf1", ".*", 1, 2, 3);
        });
    }

    @Override @Test
    public void testQueryRegroup() throws ImhotepOutOfMemoryException {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")), 1, 2);
        assertEqualGroupStats(imhotepSession -> {
            return imhotepSession.regroup(rule);
        });
    }

    @Override
    @Test
    public void testUnconditionalRegroup() throws ImhotepOutOfMemoryException {
        assertEqualGroupStats(imhotepSession -> {
            return imhotepSession.regroup(new int[]{1}, new int[]{5}, false);
        });
    }

    @Override
    @Test
    public void testStringOrRegroup() throws ImhotepOutOfMemoryException {
        assertEqualGroupStatsVoid(imhotepSession -> {
            imhotepSession.stringOrRegroup("sf1", new String[]{"1a", "a", "4a"}, 1, 2, 3);
        });
    }

    @Override
    @Test
    public void testConsolidateGroups() throws ImhotepOutOfMemoryException {
        assertEqualGroupStatsVoid(session -> {
            session.intOrRegroup(new RegroupParams(ImhotepSession.DEFAULT_GROUPS, "input1"), "metric", new long[] {2, 3}, 1, 0, 1);
            session.stringOrRegroup(new RegroupParams(ImhotepSession.DEFAULT_GROUPS, "input2"), "sf1", new String[] {"1a", "2a"}, 1, 0, 1);
            session.consolidateGroups(Lists.newArrayList("input1", "input2"), Operator.AND, ImhotepSession.DEFAULT_GROUPS);
        });
    }

    @Override
    @Test
    public void testResetGroups() throws Exception {
        assertEqualGroupStatsVoid(session -> {
            session.intOrRegroup(RegroupParams.DEFAULT, "metric", new long[] {2, 3}, 1, 0, 1);
            session.resetGroups(ImhotepSession.DEFAULT_GROUPS);
        });
    }

    @Override
    @Test
    public void testDeleteGroups() {
        // nothing really great to check here
    }
}
