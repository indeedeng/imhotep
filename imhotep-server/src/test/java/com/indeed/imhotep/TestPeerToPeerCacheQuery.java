package com.indeed.imhotep;

import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author xweng
 */
public class TestPeerToPeerCacheQuery {
    private static PeerToPeerCacheTestContext testContext;
    private static List<Host> daemonHosts;
    private static final int DURATION = 5;

    @BeforeClass
    public static void setUp() throws IOException, TimeoutException, InterruptedException {
        testContext = new PeerToPeerCacheTestContext(3);
        testContext.createDailyShard("dataset", DURATION, false);
        daemonHosts = testContext.getDaemonHosts();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        testContext.close();
    }

    @Test
    public void testDistinct() throws IOException, TimeoutException, InterruptedException {
        try (final ImhotepSession session = getSession()) {
            final GroupStatsIterator result = session.getDistinct("if1", true);
            assertTrue(result.hasNext());
            assertEquals(result.nextLong(), 0);
            assertTrue(result.hasNext());
            assertEquals(result.nextLong(), 50);
        }
    }

    @Test
    public void testRegroup() throws IOException, TimeoutException, InterruptedException, ImhotepOutOfMemoryException {
        try (final ImhotepSession session = getSession()) {
            session.metricRegroup(Collections.singletonList("shardId"), (long) 1, (long) (1 + DURATION), (long) 1, true);
            long[] result =  session.getGroupStats(Collections.singletonList("count()"));
            assertArrayEquals(new long[] {0, 20, 20, 20, 20}, result);
        }
    }

    @Test
    public void testGetFTGSIterator() throws IOException, TimeoutException, InterruptedException, ImhotepOutOfMemoryException {
        try (final ImhotepSession session = getSession()) {
            final FTGSIterator intFtgsIterator = session.getFTGSIterator(new String[] {"if1"}, new String[]{}, 100,
                    0, ImmutableList.of(ImmutableList.of("count()")), StatsSortOrder.ASCENDING);
            while (intFtgsIterator.nextField()) {
                int expectedTerm = 0;
                while (intFtgsIterator.nextTerm()) {
                    assertEquals(expectedTerm, intFtgsIterator.termIntVal());
                    while (intFtgsIterator.nextGroup()) {
                        assertEquals(1, intFtgsIterator.group());
                        assertEquals(expectedTerm < 10 ? 6 : 1, intFtgsIterator.termDocFreq());
                    }
                    expectedTerm += 1;
                }
            }

            final FTGSIterator strFtgsIterator = session.getFTGSIterator(new String[] {}, new String[]{"sf1"}, 100, ImmutableList.of(ImmutableList.of("count()")));
            while (strFtgsIterator.nextField()) {
                int expectedTerm = 0;
                while (strFtgsIterator.nextTerm()) {
                    while (strFtgsIterator.nextGroup()) {
                        assertEquals("str"+expectedTerm, strFtgsIterator.termStringVal());
                        assertEquals(20, strFtgsIterator.termDocFreq());
                    }
                    expectedTerm += 1;
                }
            }
        }
    }

    @Test
    public void testMetricFilter() throws IOException, TimeoutException, InterruptedException, ImhotepOutOfMemoryException{
        try (final ImhotepSession session = getSession()) {
            session.metricRegroup(Collections.singletonList("shardId"), (long) 1, (long) (1 + DURATION), (long) 1, true);
            session.metricFilter(Collections.singletonList("shardId"), 2, 4, false);
            final long[] stats =  session.getGroupStats(Collections.singletonList("shardId"));
            assertArrayEquals(new long[]{0, 0, 40, 60, 80}, stats);
        }
    }

    private ImhotepSession getSession() throws IOException, TimeoutException, InterruptedException {
        final ShardMasterAndImhotepDaemonClusterRunner clusterRunner = testContext.getClusterRunner();
        final ImhotepClient client = clusterRunner.createClient();
        final DateTime date = testContext.DEFAULT_SHARD_START_DATE;
        final ImhotepClient.SessionBuilder builder = client.sessionBuilder("dataset", date, date.plusDays(DURATION));

        final Host firstHost = daemonHosts.get(0);
        final List<Shard> shards = builder.getChosenShards().stream().map(shard -> shard.withOwner(firstHost)).collect(Collectors.toList());
        return builder.shardsOverride(shards).allowPeerToPeerCache(true).build();
    }
}