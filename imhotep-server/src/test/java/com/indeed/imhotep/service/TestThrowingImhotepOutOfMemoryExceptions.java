package com.indeed.imhotep.service;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TestThrowingImhotepOutOfMemoryExceptions {
    private static Path storeDir;
    private static Path tempDir;
    private static ShardMasterAndImhotepDaemonClusterRunner clusterRunner;
    private static ImhotepClient client;

    private static final String DATASET = "dataset";
    private static final DateTime START = DateTime.now().minusDays(1);
    private static final DateTime END = START.plusDays(1);

    @BeforeClass
    public static void setUp() throws IOException, TimeoutException, InterruptedException {
        storeDir = Files.createTempDirectory("temp-imhotep");
        tempDir = Files.createTempDirectory("temp-imhotep");
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                storeDir,
                tempDir,
                ImhotepShardCreator.DEFAULT
        );

        final MemoryFlamdex shard = new MemoryFlamdex();
        for (int i = 0; i < 1000; i++) {
            final FlamdexDocument document = new FlamdexDocument();
            document.addStringTerm("sf1", "sf1");
            document.addIntTerms("if1", Longs.asList(42, Long.MIN_VALUE, Long.MAX_VALUE, 0L));
            shard.addDocument(document);
        }
        clusterRunner.createDailyShard(DATASET, START, shard);

        clusterRunner.setMemoryCapacity(500);
        clusterRunner.startDaemon();
        client = clusterRunner.createClient();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        clusterRunner.stop();
        FileUtils.deleteDirectory(tempDir.toFile());
        FileUtils.deleteDirectory(storeDir.toFile());
    }

    private final List<String> STAT = Lists.newArrayList("if1");

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testGetGroupStats() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.getGroupStats(STAT);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testGetGroupStatsIterator() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.getGroupStatsIterator(STAT);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testGetFTGSIterator() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.getFTGSIterator(new String[]{"if1"}, new String[0], Collections.singletonList(STAT));
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testGetSubsetFTGSIterator() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.getSubsetFTGSIterator(Collections.singletonMap("if1", new long[]{0}), Collections.emptyMap(), Collections.singletonList(STAT));
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testMultiRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.regroup(new GroupMultiRemapRule[] {
                    new GroupMultiRemapRule(1, 1000000, new int[]{1000000}, new RegroupCondition[]{new RegroupCondition("sf1", false, 0, "sf1", false)})
            });
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testSingleRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.regroup(new GroupRemapRule[] {
                    new GroupRemapRule(1, new RegroupCondition("sf1", false, 0, "sf1", false), 1000000, 1000000)
            });
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testQueryRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.regroup(new QueryRemapRule(1, Query.newTermQuery(Term.stringTerm("sf1", "sf1")), 1000000, 1000000));
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testIntOrRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.intOrRegroup("if1", new long[]{0L}, 1, 1000000, 1000000);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testStringOrRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.stringOrRegroup("sf1", new String[]{"sf1"}, 1, 1000000, 1000000);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testRegexRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.regexRegroup("sf1", "sf.*", 1, 1000000, 1000000);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testRandomRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.randomRegroup("sf1", false, "", 0.5, 1, 1000000, 1000000);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testRandomMultiRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.randomMultiRegroup("sf1", false, "", 1, new double[]{0.5}, new int[]{1, 1000000});
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testRandomMetricRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.randomMetricRegroup(STAT, "", 0.5, 1, 1000000, 1000000);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testRandomMetricMultiRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.randomMetricMultiRegroup(STAT, "", 1, new double[]{0.5}, new int[]{1, 1000000});
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testMetricRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.metricRegroup(STAT, 0, 10, 1);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testUnconditionalRegroup() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.regroup(new int[]{1}, new int[]{1000000}, false);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testMetricFilter1() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.metricFilter(STAT, 0, 10, false);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testMetricFilter2() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            session.metricFilter(STAT, 0, 10, 1, 1, 1000000);
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testMultiFTGS() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            RemoteImhotepMultiSession.multiFtgs(
                    Collections.singletonList(new RemoteImhotepMultiSession.SessionField(session, "sf1", Collections.singletonList(STAT))),
                    Collections.singletonList(AggregateStatTree.constant(1)),
                    Collections.singletonList(AggregateStatTree.constant(true)),
                    false,
                    0,
                    -1,
                    true,
                    StatsSortOrder.UNDEFINED
            );
        }
    }

    @Test(expected = ImhotepOutOfMemoryException.class)
    public void testGetAggregateDistinct() throws ImhotepOutOfMemoryException {
        try (final ImhotepSession session = client.sessionBuilder(DATASET, START, END).build()) {
            RemoteImhotepMultiSession.aggregateDistinct(
                    Collections.singletonList(new RemoteImhotepMultiSession.SessionField(session, "sf1", Collections.singletonList(STAT))),
                    Collections.singletonList(AggregateStatTree.constant(true)),
                    false
            );
        }
    }
}
