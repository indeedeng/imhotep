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
import com.indeed.imhotep.io.RequestTools;
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
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Compares Parallel and Serial Batch execution results.
 */
public class TestCommandParallelExecution implements CommandsTest {

    private static final int INT_STRING_FIELD_COUNT = 10;
    private static final int DOCUMENT_COUNT = 100;
    private static final int NAMED_GROUP_COUNT = 20;
    private static final int MAX_TERM_VALUE = 10;
    private static final int TEST_ITERATION_COUNT = 100;

    private static final String DATASET = "dataset";
    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();
    private static final MemoryFlamdex memoryFlamdex = setFlamdex();
    private static Path tempShardDir;
    private static Path tempDir;
    private static ShardMasterAndImhotepDaemonClusterRunner clusterRunner;
    private static BatchRemoteImhotepMultiSession batchRemoteMultiSessionSerial;
    private static BatchRemoteImhotepMultiSession batchRemoteMultiSessionParallel;

    private static FlamdexDocument getNewDocument() {
        final FlamdexDocument.Builder builder = new FlamdexDocument.Builder();
        final Random randomIntGenerator = new Random();
        for (int i = 0; i < INT_STRING_FIELD_COUNT; i++) {
            builder.addIntTerm(getIntFieldName(i), randomIntGenerator.nextInt(MAX_TERM_VALUE));
        }
        for (int i = 0; i < INT_STRING_FIELD_COUNT; i++) {
            builder.addStringTerm(getStringFieldName(i), Integer.toString(randomIntGenerator.nextInt(MAX_TERM_VALUE)));
        }
        return builder.build();
    }

    private static MemoryFlamdex setFlamdex() {
        final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
        for (int i = 0; i < DOCUMENT_COUNT; i++) {
            memoryFlamdex.addDocument(getNewDocument());
        }
        return memoryFlamdex;
    }

    @BeforeClass
    public static void imhotepClusterSetup() throws IOException, TimeoutException, InterruptedException {
        tempShardDir = Files.createTempDirectory("shardDir");
        tempDir = Files.createTempDirectory("tempDir");
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(tempShardDir, tempDir);
        clusterRunner.createDailyShard(DATASET, TODAY.minusDays(1), memoryFlamdex);
        clusterRunner.startDaemon();
        clusterRunner.startDaemon();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        clusterRunner.stop();
        FileUtils.deleteDirectory(tempShardDir.toFile());
        FileUtils.deleteDirectory(tempDir.toFile());
    }

    private static String getStringFieldName(final int index) {
        return "StringField" + index;
    }

    private static String getIntFieldName(final int index) {
        return "IntField" + index;
    }

    private static String getNamedGroup(final int index) {
        return "NamedGroup" + index;
    }

    @Before
    public void testSetup() throws InterruptedException, IOException, TimeoutException {
        final ImhotepClient imhotepClient = clusterRunner.createClient();
        batchRemoteMultiSessionSerial = ((RemoteImhotepMultiSession) imhotepClient.sessionBuilder(DATASET, TODAY.minusDays(1), TODAY).build()).toBatch();
        batchRemoteMultiSessionParallel = ((RemoteImhotepMultiSession) imhotepClient.sessionBuilder(DATASET, TODAY.minusDays(1), TODAY).executeBatchInParallel(true).build()).toBatch();
    }

    private <T> void applyEachMultiSession(final ThrowingFunction<ImhotepSession, T> applyFunction) throws ImhotepOutOfMemoryException {
        applyFunction.apply(batchRemoteMultiSessionSerial);
        applyFunction.apply(batchRemoteMultiSessionParallel);
    }

    private void applyEachMultiSessionVoid(final VoidThrowingFunction<ImhotepSession> applyFunction) throws ImhotepOutOfMemoryException {
        applyFunction.apply(batchRemoteMultiSessionSerial);
        applyFunction.apply(batchRemoteMultiSessionParallel);
    }

    private <T> void applyEachNamedGroupField(final ThrowingFunction<Indices, ThrowingFunction<ImhotepSession, T>> applyFunction) throws ImhotepOutOfMemoryException {
        for (int i = 0; i < NAMED_GROUP_COUNT; i++) {
            final Indices indices = new Indices(i);
            applyEachMultiSession(applyFunction.apply(indices));
        }
    }

    private <T> void applyEachNamedGroupFieldVoid(final ThrowingFunction<Indices, VoidThrowingFunction<ImhotepSession>> applyFunction) throws ImhotepOutOfMemoryException {
        for (int i = 0; i < NAMED_GROUP_COUNT; i++) {
            final Indices indices = new Indices(i);
            applyEachMultiSessionVoid(applyFunction.apply(indices));
        }
    }

    private void assertEqualGroupStatsInternal() throws ImhotepOutOfMemoryException {
        final List<String> stats = new ArrayList<>();
        stats.add("1");
        for (int i = 0; i < NAMED_GROUP_COUNT; i++) {
            Assert.assertArrayEquals(batchRemoteMultiSessionSerial.getGroupStats(getNamedGroup(i), stats), batchRemoteMultiSessionParallel.getGroupStats(getNamedGroup(i), stats));
        }
    }

    private void assertEqualGroupStatsInternalOdd() throws ImhotepOutOfMemoryException {
        final List<String> stats = new ArrayList<>();
        stats.add("1");
        for (int i = 1; i < NAMED_GROUP_COUNT; i += 2) {
            Assert.assertArrayEquals(batchRemoteMultiSessionSerial.getGroupStats(getNamedGroup(i), stats), batchRemoteMultiSessionParallel.getGroupStats(getNamedGroup(i), stats));
        }
    }

    private void assertEqualGroupStatsVoid(final ThrowingFunction<Indices, VoidThrowingFunction<ImhotepSession>> applyFunction, final boolean checkConsolidation) throws ImhotepOutOfMemoryException {
        for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
            applyEachNamedGroupFieldVoid(applyFunction);
            assertEqualGroupStatsInternal();
            deleteAllGroups();

            if (checkConsolidation) {
                applyEachNamedGroupFieldVoid(applyFunction);
                consolidateOddEvenGroup();
                deleteEvenGroups();
                assertEqualGroupStatsInternalOdd();
                deleteOddGroups();

                applyEachNamedGroupFieldVoid(applyFunction);
                consolidateOddEvenGroup();
                assertEqualGroupStatsInternal();
                deleteAllGroups();
            }
            batchRemoteMultiSessionSerial.executeBatchNoMemoryException();
            batchRemoteMultiSessionParallel.executeBatchNoMemoryException();
        }
    }

    private <T> void assertEqualGroupStats(final ThrowingFunction<Indices, ThrowingFunction<ImhotepSession, T>> applyFunction, final boolean checkConsolidation) throws ImhotepOutOfMemoryException {
        for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
            applyEachNamedGroupField(applyFunction);
            assertEqualGroupStatsInternal();
            deleteAllGroups();

            if (checkConsolidation) {
                applyEachNamedGroupField(applyFunction);
                consolidateOddEvenGroup();
                deleteEvenGroups();
                assertEqualGroupStatsInternalOdd();
                deleteOddGroups();

                applyEachNamedGroupField(applyFunction);
                consolidateOddEvenGroup();
                assertEqualGroupStatsInternalOdd();
                deleteAllGroups();
            }
            batchRemoteMultiSessionParallel.executeBatchNoMemoryException();
            batchRemoteMultiSessionSerial.executeBatchNoMemoryException();
        }
    }

    private void deleteGroups(final int start, final int interval) throws ImhotepOutOfMemoryException {
        final List<String> toBeDeleted = new ArrayList<>();
        for (int i = start; i < TestCommandParallelExecution.NAMED_GROUP_COUNT; i += interval) {
            toBeDeleted.add(getNamedGroup(i));
        }
        applyEachMultiSessionVoid(imhotepSession -> imhotepSession.deleteGroups(toBeDeleted));
    }

    private void deleteAllGroups() throws ImhotepOutOfMemoryException {
        deleteGroups(0, 1);
    }

    private void deleteEvenGroups() throws ImhotepOutOfMemoryException {
        deleteGroups(0, 2);
    }

    private void deleteOddGroups() throws ImhotepOutOfMemoryException {
        deleteGroups(1, 2);
    }

    private void consolidateOddEvenGroup() throws ImhotepOutOfMemoryException {
        final Operator operator = new Random().nextBoolean() ? Operator.AND : Operator.OR;
        for (int i = 0; i < NAMED_GROUP_COUNT; i += 2) {
            final int finalI = i;
            applyEachMultiSessionVoid(imhotepSession -> imhotepSession.consolidateGroups(Arrays.asList(getNamedGroup(finalI), getNamedGroup(finalI + 1)), operator, getNamedGroup(finalI + 1)));
        }
    }

    @Override
    @Test
    public void testGetGroupStats() throws Exception {
    }

    @Override
    @Test
    public void testIntOrRegroup() throws ImhotepOutOfMemoryException {
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.intOrRegroup(indices.namedGroup, indices.intField, new long[]{0, 1, 2, 3, 4}, 1, 0, 1), true);
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.intOrRegroup(indices.namedGroup, indices.intField, new long[]{0, 1, 2, 3, 4}, 1, 3, 5), false);
    }

    @Override
    @Test
    public void testTargetedMetricFilter() throws Exception {
        final List<String> stat = new ArrayList<>();
        stat.add("1");
        assertEqualGroupStats(indices -> imhotepSession -> imhotepSession.metricFilter(indices.namedGroup, stat, -5, 5, 1, 0, 1), true);
        assertEqualGroupStats(indices -> imhotepSession -> imhotepSession.metricFilter(indices.namedGroup, stat, -5, 5, 1, 2, 3), false);
    }

    @Override
    @Test
    public void testMetricRegroup() throws Exception {
        final List<String> stat = new ArrayList<>();
        stat.add("1");
        assertEqualGroupStats(indices -> imhotepSession -> imhotepSession.metricRegroup(indices.namedGroup, stat, -5, 5, 1), false);
    }

    @Override
    @Test
    public void testMultiRegroup() throws Exception {
        assertEqualGroupStats(indices -> imhotepSession -> {
            final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(1, 3, new int[]{5}, new RegroupCondition[]{new RegroupCondition(indices.intField, true, 5, null, false)})};
            return imhotepSession.regroup(indices.namedGroup, rules, true);
        }, false);
    }

    @Override
    @Test
    public void testMultiRegroupMessagesSender() throws Exception {
        for (int i = 0; i < NAMED_GROUP_COUNT; i++) {
            final Indices indices = new Indices(i);
            final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(1, 1, new int[]{2, 3}, new RegroupCondition[]{
                            new RegroupCondition(indices.intField, true, 5, null, false), new RegroupCondition(indices.intField, true, 8, null, false)}),
            };
            final RequestTools.GroupMultiRemapRuleSender groupMultiRemapRuleSender = RequestTools.GroupMultiRemapRuleSender.createFromRules(Arrays.asList(rules).iterator(), true);
            batchRemoteMultiSessionParallel.regroupWithRuleSender(indices.namedGroup, groupMultiRemapRuleSender, true);
            batchRemoteMultiSessionSerial.regroupWithRuleSender(indices.namedGroup, groupMultiRemapRuleSender, true);
        }
        assertEqualGroupStatsInternal();
        deleteAllGroups();
        batchRemoteMultiSessionSerial.executeBatchNoMemoryException();
        batchRemoteMultiSessionParallel.executeBatchNoMemoryException();
    }

    @Override
    @Test
    public void testMultiRegroupMessagesIterator() throws Exception {
        assertEqualGroupStatsVoid(indices -> imhotepSession -> {
            final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(1, 2, new int[]{3, 5}, new RegroupCondition[]{
                            new RegroupCondition(indices.intField, true, 5, null, false), new RegroupCondition(indices.stringField, false, 0, "5", false)}),
            };
            final Iterator<GroupMultiRemapRule> groupMultiRemapRuleIterator = Arrays.asList(rules).iterator();
            imhotepSession.regroup(indices.namedGroup, 1, groupMultiRemapRuleIterator, false);
        }, false);
    }

    @Override
    @Test
    public void testUntargetedMetricFilter() throws Exception {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualGroupStats(indices -> imhotepSession -> imhotepSession.metricFilter(indices.namedGroup, stats, -5, 5, true), true);
    }

    @Override
    @Test
    public void testRandomMetricMultiRegroup() throws Exception {
        assertEqualGroupStatsVoid(indices -> imhotepSession -> {
            final List<String> stat = new ArrayList<>();
            stat.add(indices.intField);
            imhotepSession.randomMetricMultiRegroup(indices.namedGroup, stat, "salt", 1, new double[]{0.2, 0.4}, new int[]{2, 3, 4});
        }, false);
    }

    @Override
    @Test
    public void testRandomMetricRegroup() throws Exception {
        final List<String> stat = new ArrayList<>();
        stat.add("if1");
        assertEqualGroupStatsVoid(indices -> imhotepSession -> {
            imhotepSession.randomMetricRegroup(indices.namedGroup, stat, "salt", 0.56, 1, 0, 1);
        }, true);
        assertEqualGroupStatsVoid(indices -> imhotepSession -> {
            imhotepSession.randomMetricRegroup(indices.namedGroup, stat, "salt", 0.56, 1, 3, 4);
        }, false);
    }

    @Override
    @Test
    public void testRandomMultiRegroup() throws Exception {
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.randomMultiRegroup(indices.namedGroup, indices.intField, true, "salt", 1, new double[]{0.4, 0.8}, new int[]{0, 1, 0}), true);
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.randomMultiRegroup(indices.namedGroup, indices.intField, true, "salt", 1, new double[]{0.4, 0.8}, new int[]{1, 2, 3}), false);
    }

    @Override
    @Test
    public void testRandomRegroup() throws Exception {
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.randomRegroup(indices.namedGroup, indices.intField, true, "salt123", 0.60, 1, 0, 1), true);
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.randomRegroup(indices.namedGroup, indices.intField, true, "salt123", 0.60, 1, 3, 4), false);
    }

    @Override
    @Test
    public void testRegexRegroup() throws Exception {
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.regexRegroup(indices.namedGroup, indices.stringField, ".*", 1, 0, 1), true);
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.regexRegroup(indices.namedGroup, indices.stringField, ".*", 1, 2, 3), false);
    }

    @Override
    @Test
    public void testQueryRegroup() throws Exception {
        assertEqualGroupStats(indices -> imhotepSession -> {
            final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term(indices.intField, true, 0, "5")), 0, 1);
            return imhotepSession.regroup(indices.namedGroup, rule);
        }, true);
        assertEqualGroupStats(indices -> imhotepSession -> {
            final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term(indices.intField, true, 0, "5")), 3, 5);
            return imhotepSession.regroup(indices.namedGroup, rule);
        }, false);
    }

    @Override
    @Test
    public void testUnconditionalRegroup() throws Exception {
        assertEqualGroupStats(indices -> imhotepSession -> imhotepSession.regroup(indices.namedGroup, new int[]{1}, new int[]{5}, false), false);
        assertEqualGroupStats(indices -> imhotepSession -> imhotepSession.regroup(indices.namedGroup, new int[]{1}, new int[]{0}, false), true);
    }

    @Override
    @Test
    public void testStringOrRegroup() throws Exception {
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.stringOrRegroup(indices.namedGroup, indices.stringField, new String[]{"1", "2", "3"}, 1, 0, 1), true);
        assertEqualGroupStatsVoid(indices -> imhotepSession -> imhotepSession.stringOrRegroup(indices.namedGroup, indices.stringField, new String[]{"1", "2", "3"}, 1, 2, 3), false);
    }

    @Override
    @Test
    public void testConsolidateGroups() throws Exception {
        // already tested for all tests in assertEqualGroupStats()
    }

    @Override
    @Test
    public void testResetGroups() throws Exception {
        assertEqualGroupStatsVoid(indices -> imhotepSession -> {
            imhotepSession.intOrRegroup(indices.namedGroup, indices.intField, new long[]{0, 1, 2, 3, 4}, 1, 0, 1);
            imhotepSession.resetGroups(indices.namedGroup.getOutputGroups());
        }, true);
        assertEqualGroupStatsVoid(indices -> imhotepSession -> {
            imhotepSession.intOrRegroup(indices.namedGroup, indices.intField, new long[]{0, 1, 2, 3, 4}, 1, 3, 5);
            imhotepSession.resetGroups(indices.namedGroup.getOutputGroups());
        }, false);
    }

    @Override
    @Test
    public void testDeleteGroups() throws Exception {
        // already tested for all tests in assertEqualGroupStats()
    }

    private interface ThrowingFunction<K, V> {
        V apply(K k) throws ImhotepOutOfMemoryException;
    }

    private interface VoidThrowingFunction<K> {
        void apply(K k) throws ImhotepOutOfMemoryException;
    }

    private class Indices {
        final String intField;
        final String stringField;
        final RegroupParams namedGroup;

        private Indices(final int index) {
            intField = getIntFieldName(index % INT_STRING_FIELD_COUNT);
            stringField = getStringFieldName(index % INT_STRING_FIELD_COUNT);
            namedGroup = new RegroupParams(ImhotepSession.DEFAULT_GROUPS, getNamedGroup(index));
        }
    }
}
