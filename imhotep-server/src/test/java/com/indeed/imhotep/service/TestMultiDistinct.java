/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.RemoteImhotepMultiSession.SessionField;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.indeed.imhotep.metrics.aggregate.AggregateStatTree.constant;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatTree.stat;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jwolfe
 */

@RunWith(Parameterized.class)
public class TestMultiDistinct {
    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();

    private static final String DATASET = "dataset";

    @Parameters
    public static List<Object[]> parameters() {
        final List<Object[]> result = new ArrayList<>();
        for (final int numServers : new int[]{1, 2}) {
            result.add(new Object[] { numServers });
        }
        return result;
    }

    private final int numServers;

    public TestMultiDistinct(final int numServers) {
        this.numServers = numServers;
    }

    @Rule
    public final TemporaryFolder rootDir = new TemporaryFolder();

    private ShardMasterAndImhotepDaemonClusterRunner clusterRunner;

    @Before
    public void setUp() {
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                rootDir.newFolder("shards").toPath(),
                rootDir.newFolder("temp").toPath());
    }

    @After
    public void tearDown() throws IOException {
        clusterRunner.stop();
    }

    @Test
    public void singleDataset() throws IOException, TimeoutException, InterruptedException, ImhotepOutOfMemoryException {
        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "US")
                    .addStringTerm("q", "software engineer")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 3)
                    .build()
            );
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "US")
                    .addStringTerm("q", "java software engineer")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 1)
                    .build()
            );
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "US")
                    .addStringTerm("q", "entry level software engineer")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 0)
                    .build()
            );
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "JP")
                    .addStringTerm("q", "デザイン")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 10)
                    .build()
            );
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "JP")
                    .addStringTerm("q", "デザイン")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 2)
                    .build()
            );
            clusterRunner.createDailyShard(DATASET, TODAY.minusDays(1), memoryFlamdex);
        }

        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "AU")
                    .addStringTerm("q", "software engineer")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 3)
                    .build()
            );
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "US")
                    .addStringTerm("q", "java")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 2)
                    .build()
            );
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "US")
                    .addStringTerm("q", "java")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 2)
                    .build()
            );
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "JP")
                    .addStringTerm("q", "デザイン")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 10)
                    .build()
            );
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("country", "JP")
                    .addStringTerm("q", "デザイン")
                    .addIntTerm("impressions", 10)
                    .addIntTerm("clicks", 2)
                    .build()
            );
            clusterRunner.createDailyShard(DATASET, TODAY.minusDays(2), memoryFlamdex);
        }

        for (int i = 0; i < numServers; i++) {
            clusterRunner.startDaemon();
        }

        final List<List<String>> stats = new ArrayList<>();

        try (ImhotepClient client = clusterRunner.createClient();
             ImhotepSession session = client.sessionBuilder(DATASET, TODAY.minusDays(2), TODAY).build()) {
            // SELECT DISTINCT(country)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    singletonList(new SessionField(session, "country", stats)),
                    Lists.newArrayList(constant(true)),
                    false
            )) {
                assertEquals(2, statsIterator.getNumGroups());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(3, statsIterator.nextLong());
                assertFalse(statsIterator.hasNext());
            }

            stats.add(singletonList("count()"));
            final AggregateStatTree countStat = stat(session, stats.size() - 1);

            // SELECT DISTINCT(country HAVING count() > 2)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    singletonList(new SessionField(session, "country", stats)),
                    Lists.newArrayList(countStat.gt(constant(2))),
                    false
            )) {
                assertEquals(2, statsIterator.getNumGroups());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(2, statsIterator.nextLong());
                assertFalse(statsIterator.hasNext());
            }

            // SELECT DISTINCT(country HAVING count() > 100)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    singletonList(new SessionField(session, "country", stats)),
                    Lists.newArrayList(countStat.gt(constant(100))),
                    false
            )) {
                // trailing 0s are removed
                assertEquals(0, statsIterator.getNumGroups());
                assertFalse(statsIterator.hasNext());
            }

            // DO THEM ALL
            // SELECT DISTINCT(country),
            //        DISTINCT(country HAVING count() > 2),
            //        DISTINCT(country HAVING count() > 100)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    singletonList(new SessionField(session, "country", stats)),
                    Lists.newArrayList(
                            constant(true),
                            countStat.gt(constant(2)),
                            countStat.gt(constant(100))
                    ),
                    false
            )) {
                // 1 trailing zero removed = 3 * 2 - 1 = 5
                assertEquals(5, statsIterator.getNumGroups());
                // Group 0
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                // Group 1
                assertTrue(statsIterator.hasNext());
                assertEquals(3, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(2, statsIterator.nextLong());
                assertFalse(statsIterator.hasNext());
            }

            // deliberately don't pop to have unused stats on the stack to test
            stats.add(singletonList("clicks"));
            final AggregateStatTree clickStat = stat(session, stats.size() - 1);
            stats.add(singletonList("impressions"));
            final AggregateStatTree impressionsStat = stat(session, stats.size() - 1);

            // SELECT DISTINCT(q HAVING clicks / impressions >= 0.25)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    singletonList(new SessionField(session, "q", stats)),
                    Lists.newArrayList(clickStat.divide(impressionsStat).gte(constant(0.25))),
                    false
            )) {
                assertEquals(2, statsIterator.getNumGroups());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(2, statsIterator.nextLong());
                assertFalse(statsIterator.hasNext());
            }
        }
    }

    @Test
    public void multipleDatasets()  throws IOException, TimeoutException, InterruptedException, ImhotepOutOfMemoryException {
        final String dataset1 = "dataset1";
        final String ds1clicks = "ds1clicks";
        final String dataset2 = "dataset2";
        final String ds2clicks = "ds2clicks";

        // Two separate shards of one dataset to ensure that it will appear on
        // multiple different daemons in the multi-server case
        // US count: 140
        // US clicks: 580
        // JP count: 20
        // JP clicks: 100
        // AU count: 10
        // AU clicks: 50
        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            final BiFunction<String, Integer, FlamdexDocument> makeDoc = (country, clicks) -> new FlamdexDocument.Builder()
                    .addStringTerm("country", country)
                    .addIntTerm(ds1clicks, clicks)
                    .build();
            // US count: 115
            // US clicks: 515
            // JP count: 10
            // JP clicks: 50
            for (int i = 0; i < 100; i++) {
                memoryFlamdex.addDocument(makeDoc.apply("US", 5));
            }
            for (int i = 0; i < 15; i++) {
                memoryFlamdex.addDocument(makeDoc.apply("US", 1));
            }
            for (int i = 0; i < 10; i++) {
                memoryFlamdex.addDocument(makeDoc.apply("JP", 5));
            }
            clusterRunner.createDailyShard(dataset1, TODAY.minusDays(1), memoryFlamdex);
        }
        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            final BiFunction<String, Integer, FlamdexDocument> makeDoc = (country, clicks) -> new FlamdexDocument.Builder()
                    .addStringTerm("country", country)
                    .addIntTerm(ds1clicks, clicks)
                    .build();
            // US count: 25
            // US clicks: 65
            // JP count: 10
            // JP clicks: 50
            // AU count: 10
            // AU clicks: 50
            for (int i = 0; i < 10; i++) {
                memoryFlamdex.addDocument(makeDoc.apply("US", 5));
            }
            for (int i = 0; i < 15; i++) {
                memoryFlamdex.addDocument(makeDoc.apply("US", 1));
            }
            for (int i = 0; i < 10; i++) {
                memoryFlamdex.addDocument(makeDoc.apply("JP", 5));
            }
            for (int i = 0; i < 10; i++) {
                memoryFlamdex.addDocument(makeDoc.apply("AU", 5));
            }
            clusterRunner.createDailyShard(dataset1, TODAY.minusDays(2), memoryFlamdex);
        }

        // One single shard of a dataset to ensure a node set mismatch in the multi-server case
        // US count: 10
        // US clicks: 50
        // JP count: 50
        // JP clicks: 250
        // AU count: 0
        // AU clicks: 0
        // GB count: 1
        // GB clicks: 1
        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            final BiFunction<String, Integer, FlamdexDocument> makeDoc = (country, clicks) -> new FlamdexDocument.Builder()
                    .addStringTerm("country", country)
                    .addIntTerm(ds2clicks, clicks)
                    .build();
            for (int i = 0; i < 10; i++) {
                memoryFlamdex.addDocument(makeDoc.apply("US", 5));
            }
            for (int i = 0; i < 50; i++) {
                memoryFlamdex.addDocument(makeDoc.apply("JP", 5));
            }
            memoryFlamdex.addDocument(makeDoc.apply("GB", 1));
            clusterRunner.createDailyShard(dataset2, TODAY.minusDays(1), memoryFlamdex);
        }

        for (int i = 0; i < numServers; i++) {
            clusterRunner.startDaemon();
        }

        final List<List<String>> stats1 = new ArrayList<>();
        final List<List<String>> stats2 = new ArrayList<>();

        try (ImhotepClient client = clusterRunner.createClient();
             ImhotepSession session1 = client.sessionBuilder(dataset1, TODAY.minusDays(2), TODAY).build();
             ImhotepSession session2 = client.sessionBuilder(dataset2, TODAY.minusDays(2), TODAY).build()
        ) {
            // SELECT DISTINCT(country)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    Lists.newArrayList(
                            new SessionField(session1, "country", stats1),
                            new SessionField(session2, "country", stats2)
                    ),
                    Lists.newArrayList(constant(true)),
                    false
            )) {
                assertEquals(2, statsIterator.getNumGroups());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(4, statsIterator.nextLong());
                assertFalse(statsIterator.hasNext());
            }

            stats1.add(singletonList("count()"));
            stats2.add(singletonList("count()"));

            final AggregateStatTree count1 = stat(session1, 0);
            final AggregateStatTree count2 = stat(session2, 0);

            // SELECT DISTINCT(country) HAVING dataset1.count() > dataset2.count()
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    Lists.newArrayList(
                            new SessionField(session1, "country", stats1),
                            new SessionField(session2, "country", stats2)
                    ),
                    Lists.newArrayList(count1.gt(count2)),
                    false
            )) {
                assertEquals(2, statsIterator.getNumGroups());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(2, statsIterator.nextLong());
                assertFalse(statsIterator.hasNext());
            }

            // GROUP BY country in ("AU", "GB", "JP", "US")
            final GroupMultiRemapRule[] rules = {
                    new GroupMultiRemapRule(
                            1,
                            0,
                            new int[]{1, 2, 3, 4},
                            new RegroupCondition[]{
                                    new RegroupCondition("country", false, 0, "AU", false),
                                    new RegroupCondition("country", false, 0, "GB", false),
                                    new RegroupCondition("country", false, 0, "JP", false),
                                    new RegroupCondition("country", false, 0, "US", false)
                            }
                    )
            };
            session1.regroup(rules);
            session2.regroup(rules);

            // GROUP BY country in ("AU", "GB", "JP", "US") SELECT DISTINCT(clicks)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    Lists.newArrayList(
                            new SessionField(session1, ds1clicks, stats1),
                            new SessionField(session2, ds2clicks, stats2)
                    ),
                    Lists.newArrayList(constant(true)),
                    true
            )) {
                // groups 0 through 4
                assertEquals(5, statsIterator.getNumGroups());
                // group 0
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                // group 1 (AU)
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                // group 2 (GB)
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                // group 3 (JP)
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                // group 4 (US)
                assertTrue(statsIterator.hasNext());
                assertEquals(2, statsIterator.nextLong());

                assertFalse(statsIterator.hasNext());
            }

            final AggregateStatTree overallCount = count1.plus(count2);

            // GROUP BY country in ("AU", "GB", "JP", "US")
            // SELECT DISTINCT(clicks),
            //        DISTINCT(clicks having count() > 50),
            //        DISTINCT(clicks having count() < 15),
            //        DISTINCT(clicks having count() = 30)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    Lists.newArrayList(
                            new SessionField(session1, ds1clicks, stats1),
                            new SessionField(session2, ds2clicks, stats2)
                    ),
                    Lists.newArrayList(
                            constant(true),
                            overallCount.gt(constant(50)),
                            overallCount.lt(constant(15)),
                            overallCount.eq(constant(30))
                    ),
                    true
            )) {
                // groups 0 through 4, with no missing stats
                assertEquals(5 * 4, statsIterator.getNumGroups());

                // group 0
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());

                // group 1 (AU)
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());

                // group 2 (GB)
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());

                // group 3 (JP)
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());

                // group 4 (US)
                assertTrue(statsIterator.hasNext());
                assertEquals(2, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(0, statsIterator.nextLong());
                assertTrue(statsIterator.hasNext());
                assertEquals(1, statsIterator.nextLong());

                assertFalse(statsIterator.hasNext());
            }
        }
    }

    @Test
    public void testWindowedDistinct() throws IOException, TimeoutException, InterruptedException, ImhotepOutOfMemoryException {
        final String dataset = "windowDistinctDataset";

        final Map<String, int[]> countries = ImmutableMap.of(
                "AQ", new int[]{1,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0},
                "AU", new int[]{0,0,0,0,1,1,1,1,0,1,1,1,0,0,0,0,0,0,0,0,1,1,1,0},
                "GB", new int[]{1,1,1,0,0,0,0,0,1,1,1,1,0,0,0,0,0,1,0,0,0,0,0,0},
                "JP", new int[]{1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1},
                "US", new int[]{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}
        );

        //         new int[]  {1,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0},
        //         new int[]  {0,0,0,0,1,1,1,1,0,1,1,1,0,0,0,0,0,0,0,0,1,1,1,0},
        //         new int[]  {1,1,1,0,0,0,0,0,1,1,1,1,0,0,0,0,0,1,0,0,0,0,0,0},
        //         new int[]  {1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1},
        //         new int[]  {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}
        final long[] rawDistincts =
                new long[] {0, 4,3,3,3,2,2,2,2,2,3,3,3,1,1,1,1,1,2,2,2,4,3,3,2};
        final long[] distinctWindow2 =
                new long[] {0, 4,4,3,4,4,2,2,2,3,3,3,3,3,1,1,1,1,2,3,2,4,4,3,3};
        final long[] distinctWindow2NoonGap =
                new long[] {0, 4,4,3,4,4,2,2,2,3,3,3,3,1,1,1,1,1,2,3,2,4,4,3,3};
        // How many countries show up at least twice in a rolling window of size 5
        final long[] rollingTwoOfFive =
                new long[] {0, 0,3,3,4,4,4,3,2,2,3,3,3,3,3,3,1,1,1,1,2,2,3,3,3};
        final long[] rollingTwoOfFiveNoonGap =
                new long[] {0, 0,3,3,4,4,4,3,2,2,3,3,3,0,1,1,1,1,1,1,2,2,3,3,3};

        for (final int[] ints : countries.values()) {
            Preconditions.checkState(ints.length == 24);
        }

        for (int i = 0; i < 24; i++) {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            final DateTime hourStart = TODAY.plusHours(i);
            final Function<String, FlamdexDocument> makeDoc = (country) -> new FlamdexDocument.Builder()
                    .addStringTerm("country", country)
                    .addIntTerm("unixtime", hourStart.getMillis() / 1000)
                    .build();
            for (final Map.Entry<String, int[]> entry : countries.entrySet()) {
                if (entry.getValue()[i] == 1) {
                    memoryFlamdex.addDocument(makeDoc.apply(entry.getKey()));
                }
            }
            clusterRunner.createHourlyShard(dataset, hourStart, memoryFlamdex);
        }

        for (int i = 0; i < numServers; i++) {
            clusterRunner.startDaemon();
        }

        final List<List<String>> stats = new ArrayList<>();

        try (ImhotepClient client = clusterRunner.createClient();
             ImhotepSession session = client.sessionBuilder(dataset, TODAY, TODAY.plusDays(1)).build()) {

            session.metricRegroup(singletonList("unixtime"), TODAY.getMillis() / 1000, TODAY.plusDays(1).getMillis() / 1000, (long) 3600);

            final List<SessionField> sessionField = singletonList(new SessionField(session, "country", stats));
            final int[] parentGroups = {0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

            // GROUP BY TIME(1h) SELECT DISTINCT_WINDOW(1, country)
            testIntDistinctWindow(
                    rawDistincts,
                    sessionField,
                    singletonList(constant(true)),
                    singletonList(1),
                    parentGroups
            );
            // GROUP BY TIME(1h) SELECT DISTINCT_WINDOW(2, country)
            testIntDistinctWindow(
                    distinctWindow2,
                    sessionField,
                    singletonList(constant(true)),
                    singletonList(2),
                    parentGroups
            );

            stats.add(singletonList("count()"));

            // GROUP BY TIME(1h) SELECT DISTINCT_WINDOW(5, country having count() >= 2)
            final AggregateStatTree countGte2 = AggregateStatTree.stat(session, 0).gte(AggregateStatTree.constant(2));
            testIntDistinctWindow(
                    rollingTwoOfFive,
                    sessionField,
                    singletonList(countGte2),
                    singletonList(5),
                    parentGroups
            );

            final LongArrayList interleaved = new LongArrayList();
            for (int i = 0; i < rawDistincts.length; i++) {
                for (final long[] ints : Arrays.asList(rawDistincts, distinctWindow2, rollingTwoOfFive)) {
                    interleaved.add(ints[i]);
                }
            }

            while (interleaved.get(interleaved.size() - 1) == 0) {
                interleaved.popLong();
            }

            // GROUP BY TIME(1h)
            // SELECT
            //  DISTINCT_WINDOW(1, country),
            //  DISTINCT_WINDOW(2, country),
            //  DISTINCT_WINDOW(5, country HAVING COUNT() >= 2)
            testIntDistinctWindow(
                    interleaved.toLongArray(),
                    sessionField,
                    Arrays.asList(constant(true), constant(true), countGte2),
                    Arrays.asList(1, 2, 5),
                    parentGroups
            );

            // GROUP BY time < noon, time(1h) SELECT DISTINCT_WINDOW(2, country),
            final int[] noonSplitParentGroups = {0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2};
            // GROUP BY TIME(1h) SELECT DISTINCT_WINDOW(2, country)
            testIntDistinctWindow(
                    distinctWindow2NoonGap,
                    sessionField,
                    singletonList(constant(true)),
                    singletonList(2),
                    noonSplitParentGroups
            );

            // GROUP BY time < noon, TIME(1h) SELECT DISTINCT_WINDOW(5, country having count() >= 2)
            testIntDistinctWindow(
                    rollingTwoOfFiveNoonGap,
                    sessionField,
                    singletonList(countGte2),
                    singletonList(5),
                    noonSplitParentGroups
            );
        }
    }

    @Test
    public void testWindowDistinctMultiDatasets() throws IOException, TimeoutException, InterruptedException, ImhotepOutOfMemoryException {
        final String dataset1 = "windowDistinctDataset2_1";
        final String dataset2 = "windowDistinctDataset2_2";

        final int[] counts1 = new int[]{5, 2, 0, 2, 7, 1, 0};
        // WINDOW(2):                  {5, 7, 2, 2, 9, 8, 1}
        final int[] counts2 = new int[]{1, 2, 1, 3, 1, 1, 1};
        // WINDOW(2):                  {1, 3, 3, 4, 4, 2, 2}
        final long[] window2GT1 = new long[]{0, 0, 0, 1, 1, 0, 0, 1};

        for (int i = 0; i < 7; i++) {
            final MemoryFlamdex memoryFlamdex1 = new MemoryFlamdex();
            final MemoryFlamdex memoryFlamdex2 = new MemoryFlamdex();
            final DateTime dayStart = TODAY.plusDays(i);
            final FlamdexDocument document = new FlamdexDocument.Builder()
                    .addStringTerm("country", "US")
                    .addIntTerm("unixtime", dayStart.getMillis() / 1000)
                    .build();
            for (int j = 0; j < counts1[i]; j++) {
                memoryFlamdex1.addDocument(document);
            }
            for (int j = 0; j < counts2[i]; j++) {
                memoryFlamdex2.addDocument(document);
            }
            clusterRunner.createDailyShard(dataset1, dayStart, memoryFlamdex1);
            clusterRunner.createDailyShard(dataset2, dayStart, memoryFlamdex2);
        }

        for (int i = 0; i < numServers; i++) {
            clusterRunner.startDaemon();
        }

        final List<List<String>> stats1 = new ArrayList<>();
        final List<List<String>> stats2 = new ArrayList<>();

        try (final ImhotepClient client = clusterRunner.createClient();
             final ImhotepSession session1 = client.sessionBuilder(dataset1, TODAY, TODAY.plusDays(8)).build();
             final ImhotepSession session2 = client.sessionBuilder(dataset2, TODAY, TODAY.plusDays(8)).build();
        ) {
            session1.metricRegroup(singletonList("unixtime"), TODAY.getMillis() / 1000, TODAY.plusDays(8).getMillis() / 1000, (long) (3600 * 24));
            session2.metricRegroup(singletonList("unixtime"), TODAY.getMillis() / 1000, TODAY.plusDays(8).getMillis() / 1000, (long) (3600 * 24));

            stats1.add(singletonList("count()"));
            final AggregateStatTree count1 = AggregateStatTree.stat(session1, stats1.size() - 1);
            stats2.add(singletonList("count()"));
            final AggregateStatTree count2 = AggregateStatTree.stat(session2, stats2.size() - 1);

            final List<SessionField> sessionFields = Lists.newArrayList(
                    new SessionField(session1, "country", stats1),
                    new SessionField(session2, "country", stats2)
            );
            final int[] parentGroups = {0, 1, 1, 1, 1, 1, 1, 1};

            testIntDistinctWindow(
                    window2GT1,
                    sessionFields,
                    singletonList(count2.gt(count1)),
                    singletonList(2),
                    parentGroups
            );

            stats1.add(singletonList("count()"));
            final AggregateStatTree count1_2 = AggregateStatTree.stat(session1, stats1.size() - 1);
            stats1.add(singletonList("0"));
            final AggregateStatTree zero1 = AggregateStatTree.stat(session1, stats1.size() - 1);

            // same thing but using (count() + count() + [0]) / 2 to push multiple stats to one of the sessions.
            testIntDistinctWindow(
                    window2GT1,
                    sessionFields,
                    singletonList(count2.gt(count1.plus(count1_2).plus(zero1).divide(AggregateStatTree.constant(2)))),
                    singletonList(2),
                    parentGroups
            );

            // test some weird crazy thing from IMTEPD-501
            testIntDistinctWindow(
                    new long[]{0, 1,1,1,1,1,1,1},
                    Lists.newArrayList(
                            new SessionField(session1, "country", Arrays.asList(singletonList("count()"), singletonList("count()"))),
                            new SessionField(session2, "country", singletonList(singletonList("0")))
                    ),
                    singletonList(AggregateStatTree.stat(session1, 0).eq(AggregateStatTree.stat(session1, 1))),
                    singletonList(2),
                    parentGroups
            );
        }
    }

    private void testIntDistinctWindow(final long[] expected, final List<SessionField> sessions, final List<AggregateStatTree> filters, final List<Integer> windowSizes, final int[] parentGroups) throws IOException, ImhotepOutOfMemoryException {
        try (final GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                sessions,
                filters,
                windowSizes,
                false,
                parentGroups
        )) {
            final long[] stats = new long[statsIterator.getNumGroups()];
            for (int i = 0; i < stats.length; i++) {
                assertTrue(statsIterator.hasNext());
                stats[i] = statsIterator.nextLong();
            }
            assertFalse(statsIterator.hasNext());
            if (!Arrays.equals(expected, stats)) {
                System.out.println("expected = " + Arrays.toString(expected));
                System.out.println("stats    = " + Arrays.toString(stats));
            }
            assertArrayEquals(expected, stats);
        }
    }
}
