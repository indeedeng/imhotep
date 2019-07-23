package com.indeed.imhotep.service;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

    private List<ImhotepSession> sessions = new ArrayList<>();

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

    @Before
    public void setup() {
        sessions.add(client.sessionBuilder(DATASET, START, END).build());
        sessions.add(client.sessionBuilder(DATASET, START, END).useBatch(true).build());
        sessions.add(client.sessionBuilder(DATASET, START, END).useBatch(true).executeBatchInParallel(true).build());
    }

    @After
    public void closeSessions() {
        for (final ImhotepSession session: sessions) {
            session.close();
        }
        sessions.clear();
    }

    private interface ThrowingFunction<K, V> {
        V apply(K k) throws ImhotepOutOfMemoryException;
    }

    private interface VoidThrowingFunction<K> {
        void apply(K k) throws ImhotepOutOfMemoryException;
    }

    private <T> void assertErrorOnAllSessions(final ThrowingFunction<ImhotepSession, T> applyFunction) {
        for (final ImhotepSession imhotepSession: sessions) {
            try {
                applyFunction.apply(imhotepSession);
            } catch (final Exception e) {
                if (!(e instanceof ImhotepOutOfMemoryException)) {
                    throw new AssertionError("Expected to throw ImhotepOutOfMemoryException");
                }
            }
        }
    }

    private <T> void assertErrorOnAllSessionsVoid(final VoidThrowingFunction<ImhotepSession> applyFunction) {
        for (final ImhotepSession imhotepSession: sessions) {
            try {
                applyFunction.apply(imhotepSession);
            } catch (final Exception e) {
                if (!(e instanceof ImhotepOutOfMemoryException)) {
                    throw new AssertionError("Expected to throw ImhotepOutOfMemoryException");
                }
            }
        }
    }

    @Test
    public void testGetGroupStats() {
        assertErrorOnAllSessions(session -> session.getGroupStats(STAT));
    }

    @Test
    public void testGetGroupStatsIterator() {
        assertErrorOnAllSessions(session -> session.getGroupStatsIterator(STAT));
    }

    @Test
    public void testGetFTGSIterator() {
        assertErrorOnAllSessions(session -> session.getFTGSIterator(new String[]{"if1"}, new String[0], Collections.singletonList(STAT)));
    }

    @Test
    public void testGetSubsetFTGSIterator() {
        assertErrorOnAllSessions(session -> session.getSubsetFTGSIterator(Collections.singletonMap("if1", new long[]{0}), Collections.emptyMap(), Collections.singletonList(STAT)));
    }

    @Test
    public void testMultiRegroup() {
        assertErrorOnAllSessions(session -> session.regroup(new GroupMultiRemapRule[] {
                    new GroupMultiRemapRule(1, 1000000, new int[]{1000000}, new RegroupCondition[]{new RegroupCondition("sf1", false, 0, "sf1", false)})
            }));
    }

    @Test
    public void testQueryRegroup() {
        assertErrorOnAllSessions(session -> session.regroup(new QueryRemapRule(1, Query.newTermQuery(Term.stringTerm("sf1", "sf1")), 1000000, 1000000)));
    }

    @Test
    public void testIntOrRegroup() {
        assertErrorOnAllSessionsVoid(session -> session.intOrRegroup("if1", new long[]{0L}, 1, 1000000, 1000000));
    }

    @Test
    public void testStringOrRegroup() {
        assertErrorOnAllSessionsVoid(session -> session.stringOrRegroup("sf1", new String[]{"sf1"}, 1, 1000000, 1000000));
    }

    @Test
    public void testRegexRegroup() {
        assertErrorOnAllSessionsVoid(session -> session.regexRegroup("sf1", "sf.*", 1, 1000000, 1000000));
    }

    @Test
    public void testRandomRegroup() {
         assertErrorOnAllSessionsVoid(session -> session.randomRegroup("sf1", false, "", 0.5, 1, 1000000, 1000000));
    }

    @Test
    public void testRandomMetricRegroup() {
        assertErrorOnAllSessionsVoid(session -> session.randomMetricRegroup(STAT, "", 0.5, 1, 1000000, 1000000));
    }

    @Test
    public void testRandomMetricMultiRegroup() {
        assertErrorOnAllSessionsVoid(session -> session.randomMetricMultiRegroup(STAT, "", 1, new double[]{0.5}, new int[]{1, 1000000}));
    }

    @Test
    public void testMetricRegroup() {
        assertErrorOnAllSessionsVoid(session -> session.metricRegroup(STAT, 0, 10, 1));
    }

    @Test
    public void testUnconditionalRegroup() {
        assertErrorOnAllSessionsVoid(session -> session.regroup(new int[]{1}, new int[]{1000000}, false));
    }

    @Test
    public void testMetricFilter1() {
        assertErrorOnAllSessionsVoid(session -> session.metricFilter(STAT, 0, 10, false));
    }

    @Test
    public void testMetricFilter2() {
        assertErrorOnAllSessionsVoid(session -> session.metricFilter(STAT, 0, 10, 1, 1, 1000000));
    }

    @Test
    public void testMultiFTGS() {
        assertErrorOnAllSessions(session ->
                RemoteImhotepMultiSession.multiFtgs(
                Collections.singletonList(new RemoteImhotepMultiSession.SessionField(session, "sf1", Collections.singletonList(STAT))),
                Collections.singletonList(AggregateStatTree.constant(1)),
                Collections.singletonList(AggregateStatTree.constant(true)),
                false,
                0,
                -1,
                true,
                StatsSortOrder.UNDEFINED
        ));
    }

    @Test
    public void testGetAggregateDistinct() {
        assertErrorOnAllSessions(session ->
                RemoteImhotepMultiSession.aggregateDistinct(
                Collections.singletonList(new RemoteImhotepMultiSession.SessionField(session, "sf1", Collections.singletonList(STAT))),
                Collections.singletonList(AggregateStatTree.constant(true)),
                false
        ));
    }
}
