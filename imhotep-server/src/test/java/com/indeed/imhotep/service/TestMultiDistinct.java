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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import static com.indeed.imhotep.metrics.aggregate.AggregateStatTree.constant;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatTree.stat;
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
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(rootDir.newFolder("shards"), rootDir.newFolder("temp"));
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

        try (ImhotepClient client = clusterRunner.createClient();
             ImhotepSession session = client.sessionBuilder(DATASET, TODAY.minusDays(2), TODAY).build()) {
            // SELECT DISTINCT(country)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    Collections.singletonList(new SessionField(session, "country")),
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

            final int countIndex = session.pushStat("count()") - 1;
            final AggregateStatTree countStat = stat(session, countIndex);

            // SELECT DISTINCT(country HAVING count() > 2)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    Collections.singletonList(new SessionField(session, "country")),
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
                    Collections.singletonList(new SessionField(session, "country")),
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
                    Collections.singletonList(new SessionField(session, "country")),
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
            final int clicksIndex = session.pushStat("clicks") - 1;
            final int impressionsIndex = session.pushStat("impressions") - 1;

            final AggregateStatTree clickStat = stat(session, clicksIndex);
            final AggregateStatTree impressionsStat = stat(session, impressionsIndex);

            // SELECT DISTINCT(q HAVING clicks / impressions >= 0.25)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    Collections.singletonList(new SessionField(session, "q")),
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

        try (ImhotepClient client = clusterRunner.createClient();
             ImhotepSession session1 = client.sessionBuilder(dataset1, TODAY.minusDays(2), TODAY).build();
             ImhotepSession session2 = client.sessionBuilder(dataset2, TODAY.minusDays(2), TODAY).build()
        ) {
            // SELECT DISTINCT(country)
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    Lists.newArrayList(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
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

            session1.pushStat("count()");
            session2.pushStat("count()");

            final AggregateStatTree count1 = stat(session1, 0);
            final AggregateStatTree count2 = stat(session2, 0);

            // SELECT DISTINCT(country) HAVING dataset1.count() > dataset2.count()
            try (GroupStatsIterator statsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                    Lists.newArrayList(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
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
                            new SessionField(session1, ds1clicks),
                            new SessionField(session2, ds2clicks)
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
                            new SessionField(session1, ds1clicks),
                            new SessionField(session2, ds2clicks)
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
}
