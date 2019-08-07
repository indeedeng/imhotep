package com.indeed.imhotep.service;

import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.FTGIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.indeed.imhotep.FTGSIteratorTestUtils.*;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatTree.stat;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestAggregateBucket {
    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();
    private static final String DATASET1 = "dataset1";
    private static final String DATASET2 = "dataset2";

    @Parameterized.Parameters
    public static List<Object[]> parameters() {
        final List<Object[]> result = new ArrayList<>();
        for (final int numServers : new int[]{1, 2}) {
            result.add(new Object[]{numServers});
        }
        return result;
    }

    private final int numServers;

    public TestAggregateBucket(final int numServers) {
        this.numServers = numServers;
    }

    @Rule
    public final TemporaryFolder rootDir = new TemporaryFolder();
    private ShardMasterAndImhotepDaemonClusterRunner clusterRunner;

    @Before
    public void setUp() throws IOException, TimeoutException, InterruptedException {
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                rootDir.newFolder("shards").toPath(),
                rootDir.newFolder("temp").toPath());
        createDatasets(clusterRunner);

        for (int i = 0; i < numServers; i++) {
            clusterRunner.startDaemon();
        }
    }

    @After
    public void tearDown() throws IOException {
        clusterRunner.stop();
    }

    @Test
    public void testAggregateBucketRegroup() throws ImhotepOutOfMemoryException, InterruptedException, IOException, TimeoutException {
        try (final ImhotepClient client = clusterRunner.createClient()) {
            try (final ImhotepSession session = client.sessionBuilder(DATASET1, TODAY.minusDays(2), TODAY).build()) {
                final List<List<String>> stats = new ArrayList<>();
                stats.add(singletonList("count()"));
                final AggregateStatTree count = stat(session, 0);
                try (final FTGAIterator iterator = aggregateBucketRegroupAndGetFTGA(
                        singletonList(new RemoteImhotepMultiSession.PerSessionFTGSInfo(session, "mod3", stats)),
                        Optional.empty(),
                        count,
                        true,
                        2,
                        4,
                        2,
                        false,
                        false
                )) {
                    expectFirstIntField(iterator, "magic");
                    expectIntTerm(iterator, 0, 4);
                    expectGroup(iterator, 4, new double[]{4});
                    expectIntTerm(iterator, 1, 3);
                    expectGroup(iterator, 2, new double[]{3});
                    expectIntTerm(iterator, 2, 3);
                    expectGroup(iterator, 2, new double[]{3});
                    expectEnd(iterator);
                }
            }
            try (final ImhotepSession session = client.sessionBuilder(DATASET1, TODAY.minusDays(2), TODAY).build()) {
                final List<List<String>> stats = new ArrayList<>();
                stats.add(singletonList("count()"));
                final AggregateStatTree count = stat(session, 0);
                try (final FTGAIterator iterator = aggregateBucketRegroupAndGetFTGA(
                        singletonList(new RemoteImhotepMultiSession.PerSessionFTGSInfo(session, "mod3str", stats)),
                        Optional.empty(),
                        count,
                        false,
                        3,
                        5,
                        2,
                        false,
                        false
                )) {
                    expectFirstStrField(iterator, "magic");
                    expectStrTerm(iterator, "0", 4);
                    expectGroup(iterator, 2, new double[]{4});
                    expectStrTerm(iterator, "1", 3);
                    expectGroup(iterator, 1, new double[]{3});
                    expectStrTerm(iterator, "2", 3);
                    expectGroup(iterator, 1, new double[]{3});
                    expectEnd(iterator);
                }
            }
            try (
                    final ImhotepSession session1 = client.sessionBuilder(DATASET1, TODAY.minusDays(2), TODAY).build();
                    final ImhotepSession session2 = client.sessionBuilder(DATASET2, TODAY.minusDays(2), TODAY).build()
            ) {
                final List<List<String>> stats = new ArrayList<>();
                stats.add(singletonList("mod3"));
                stats.add(singletonList("count()"));
                final AggregateStatTree mod3sum = stat(session1, 0).plus(stat(session2, 0));
                final AggregateStatTree totalCount = stat(session1, 1).plus(stat(session2, 1));
                try (final FTGAIterator iterator = aggregateBucketRegroupAndGetFTGA(
                        asList(
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session1, "mod3str", stats),
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session2, "mod3str", stats)
                        ),
                        Optional.empty(),
                        mod3sum.divide(totalCount),
                        false,
                        1,
                        2,
                        1,
                        false,
                        true
                )) {
                    expectFirstStrField(iterator, "magic");
                    expectStrTerm(iterator, "0", 8);
                    expectGroup(iterator, 2, new double[]{0});
                    expectStrTerm(iterator, "1", 6);
                    expectGroup(iterator, 1, new double[]{1});
                    expectStrTerm(iterator, "2", 6);
                    expectGroup(iterator, 2, new double[]{2});
                    expectEnd(iterator);
                }
            }
            try (
                    final ImhotepSession session1 = client.sessionBuilder(DATASET1, TODAY.minusDays(2), TODAY).build();
                    final ImhotepSession session2 = client.sessionBuilder(DATASET2, TODAY.minusDays(2), TODAY).build()
            ) {
                session1.regroup(new int[]{1}, new int[]{0}, true);
                session2.regroup(new int[]{1}, new int[]{2}, true);

                final List<List<String>> stats = new ArrayList<>();
                stats.add(singletonList("mod3"));
                stats.add(singletonList("count()"));
                final AggregateStatTree mod3sum = stat(session1, 0).plus(stat(session2, 0));
                final AggregateStatTree totalCount = stat(session1, 1).plus(stat(session2, 1));
                try (final FTGAIterator iterator = aggregateBucketRegroupAndGetFTGA(
                        asList(
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session1, "mod3str", stats),
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session2, "mod3str", stats)
                        ),
                        Optional.empty(),
                        mod3sum.divide(totalCount),
                        false,
                        1,
                        2,
                        1,
                        false,
                        true
                )) {
                    expectFirstStrField(iterator, "magic");
                    expectStrTerm(iterator, "0", 4);
                    expectGroup(iterator, 4, new double[]{0});
                    expectStrTerm(iterator, "1", 3);
                    expectGroup(iterator, 3, new double[]{1});
                    expectStrTerm(iterator, "2", 3);
                    expectGroup(iterator, 4, new double[]{2});
                    expectEnd(iterator);
                }
            }
            try (
                    final ImhotepSession session1 = client.sessionBuilder(DATASET1, TODAY.minusDays(2), TODAY).build();
                    final ImhotepSession session2 = client.sessionBuilder(DATASET2, TODAY.minusDays(2), TODAY).build()
            ) {
                session1.regroup(new int[]{1}, new int[]{0}, true);
                session2.regroup(new int[]{1}, new int[]{2}, true);

                final List<List<String>> stats = new ArrayList<>();
                stats.add(singletonList("mod3"));
                stats.add(singletonList("count()"));
                final AggregateStatTree mod3sum = stat(session1, 0).plus(stat(session2, 0));
                final AggregateStatTree totalCount = stat(session1, 1).plus(stat(session2, 1));
                try (final FTGAIterator iterator = aggregateBucketRegroupAndGetFTGA(
                        asList(
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session1, "mod3str", stats),
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session2, "mod3str", stats)
                        ),
                        Optional.of(mod3sum.eq(totalCount)),
                        mod3sum.divide(totalCount),
                        false,
                        1,
                        2,
                        1,
                        false,
                        true
                )) {
                    expectFirstStrField(iterator, "magic");
                    expectStrTerm(iterator, "0", 4);
                    expectGroup(iterator, 4, new double[]{0});
                    expectStrTerm(iterator, "1", 3);
                    expectGroup(iterator, 3, new double[]{1});
                    expectStrTerm(iterator, "2", 3);
                    expectGroup(iterator, 4, new double[]{2});
                    expectEnd(iterator);
                }
                session1.resetGroups();
                session2.resetGroups();
                session1.regroup(new int[]{1}, new int[]{0}, true);
                session2.regroup(new int[]{1}, new int[]{2}, true);
                try (final FTGAIterator iterator = aggregateBucketRegroupAndGetFTGA(
                        asList(
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session1, "mod3str", stats),
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session2, "mod3str", stats)
                        ),
                        Optional.of(mod3sum.eq(totalCount)),
                        mod3sum.divide(totalCount),
                        false,
                        1,
                        2,
                        1,
                        false,
                        false
                )) {
                    expectFirstStrField(iterator, "magic");
                    expectStrTerm(iterator, "1", 3);
                    expectGroup(iterator, 4, new double[]{1});
                    expectEnd(iterator);
                }
                session1.resetGroups();
                session2.resetGroups();
                session1.regroup(new int[]{1}, new int[]{0}, true);
                session2.regroup(new int[]{1}, new int[]{2}, true);
                try (final FTGAIterator iterator = aggregateBucketRegroupAndGetFTGA(
                        asList(
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session1, "mod3str", stats),
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session2, "mod3str", stats)
                        ),
                        Optional.of(mod3sum.eq(totalCount)),
                        mod3sum.divide(totalCount),
                        false,
                        1,
                        2,
                        1,
                        true,
                        false
                )) {
                    expectFirstStrField(iterator, "magic");
                    expectStrTerm(iterator, "1", 3);
                    expectGroup(iterator, 2, new double[]{1});
                    expectEnd(iterator);
                }
            }
        }
    }

    @Test
    public void testLocalNumGroupsLessThanGlobal() throws InterruptedException, IOException, TimeoutException, ImhotepOutOfMemoryException {
        try (final ImhotepClient client = clusterRunner.createClient()) {
            try (
                    final ImhotepSession session1 = client.sessionBuilder(DATASET1, TODAY.minusDays(2), TODAY).build();
                    final ImhotepSession session2 = client.sessionBuilder(DATASET2, TODAY.minusDays(2), TODAY).build()
            ) {
                // global numGroups = 3 but session1 has 2.
                session1.regroup(new int[]{1}, new int[]{1}, true);
                session2.regroup(new int[]{1}, new int[]{2}, true);

                final List<List<String>> stats = new ArrayList<>();
                stats.add(singletonList("mod3"));
                stats.add(singletonList("count()"));
                final AggregateStatTree mod3sum = stat(session1, 0).plus(stat(session2, 0));
                final AggregateStatTree totalCount = stat(session1, 1).plus(stat(session2, 1));
                try (final FTGAIterator iterator = aggregateBucketRegroupAndGetFTGA(
                        asList(
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session1, "mod3str", stats),
                                new RemoteImhotepMultiSession.PerSessionFTGSInfo(session2, "mod3str", stats)
                        ),
                        Optional.empty(),
                        mod3sum.divide(totalCount),
                        false,
                        1,
                        2,
                        1,
                        false,
                        true
                )) {
                    expectFirstStrField(iterator, "magic");
                    expectStrTerm(iterator, "0", 8);
                    expectGroup(iterator, 2, new double[]{0});
                    expectGroup(iterator, 4, new double[]{0});
                    expectStrTerm(iterator, "1", 6);
                    expectGroup(iterator, 1, new double[]{1});
                    expectGroup(iterator, 3, new double[]{1});
                    expectStrTerm(iterator, "2", 6);
                    expectGroup(iterator, 2, new double[]{2});
                    expectGroup(iterator, 4, new double[]{2});
                    expectEnd(iterator);
                }
            }
        }
    }

    private static void expectFirstIntField(final FTGIterator iter, final String field) {
        assertTrue(iter.nextField());
        assertEquals(field, iter.fieldName());
        assertTrue(iter.fieldIsIntType());
    }

    private static void expectFirstStrField(final FTGIterator iter, final String field) {
        assertTrue(iter.nextField());
        assertEquals(field, iter.fieldName());
        assertFalse(iter.fieldIsIntType());
    }

    private static FTGAIterator aggregateBucketRegroupAndGetFTGA(
            final List<RemoteImhotepMultiSession.PerSessionFTGSInfo> sessionsWithFields,
            final Optional<AggregateStatTree> filter,
            final AggregateStatTree metric,
            final boolean isIntField,
            final double min,
            final double max,
            final int numBuckets,
            final boolean excludeGutters,
            final boolean withDefault
    ) throws ImhotepOutOfMemoryException {
        RemoteImhotepMultiSession.aggregateBucketRegroup(sessionsWithFields, filter, metric, isIntField, min, max, numBuckets, excludeGutters, withDefault);
        return RemoteImhotepMultiSession.multiFtgs(sessionsWithFields, singletonList(metric), emptyList(), isIntField, 0, -1, true, StatsSortOrder.UNDEFINED);
    }


    private static void createDatasets(final ShardMasterAndImhotepDaemonClusterRunner clusterRunner) throws IOException {
        clusterRunner.createDailyShard(DATASET1, TODAY.minusDays(1), createDataset());
        clusterRunner.createDailyShard(DATASET2, TODAY.minusDays(1), createDataset());
    }

    private static MemoryFlamdex createDataset() {
        final Function<Integer, FlamdexDocument> buildDocument = id -> {
            return new FlamdexDocument.Builder()
                    .addIntTerm("mod3", id % 3)
                    .addStringTerm("mod3str", String.valueOf(id % 3))
                    .addIntTerm("mod5", id % 5)
                    .addStringTerm("mod5str", String.valueOf(id % 5))
                    .addIntTerm("id", id)
                    .build();
        };
        final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
        for (int i = 0; i < 10; ++i) {
            memoryFlamdex.addDocument(buildDocument.apply(i));
        }
        return memoryFlamdex;
    }
}
