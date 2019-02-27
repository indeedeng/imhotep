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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.RemoteImhotepMultiSession.SessionField;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.indeed.imhotep.FTGSIteratorTestUtils.expectEnd;
import static com.indeed.imhotep.FTGSIteratorTestUtils.expectGroup;
import static com.indeed.imhotep.FTGSIteratorTestUtils.expectStrTerm;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatTree.constant;
import static com.indeed.imhotep.metrics.aggregate.AggregateStatTree.stat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jwolfe
 */

@RunWith(Parameterized.class)
public class TestMultiFTGS {
    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();

    private static final String DATASET = "dataset";

    @Parameters
    public static List<Object[]> parameters() {
        final List<Object[]> result = new ArrayList<>();
        for (final boolean sorted : new boolean[]{true, false}) {
            for (final int numServers : new int[]{1, 2}) {
                result.add(new Object[] { sorted, numServers });
            }
        }
        return result;
    }

    private final boolean sortedFTGS;
    private final int numServers;

    public TestMultiFTGS(final boolean sortedFTGS, final int numServers) {
        this.sortedFTGS = sortedFTGS;
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

            session.pushStat("count()");

            final AggregateStatTree count = stat(session, 0);

            // GROUP BY country SELECT count()
            try (FTGAIterator iterator = multiFtgs(
                    Collections.singletonList(new SessionField(session, "country")),
                    Lists.newArrayList(count),
                    Lists.newArrayList(),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"AU", 1);
                expectGroup(iterator, 1, new double[]{1});
                expectStrTerm(iterator,"JP", 4);
                expectGroup(iterator, 1, new double[]{4});
                expectStrTerm(iterator,"US", 5);
                expectGroup(iterator, 1, new double[]{5});

                expectEnd(iterator);
            }

            // GROUP BY country HAVING count() > 2, SELECT count()
            try (FTGAIterator iterator = multiFtgs(
                    Collections.singletonList(new SessionField(session, "country")),
                    Lists.newArrayList(count),
                    Lists.newArrayList(count.gt(constant(2.0))),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"JP", 4);
                expectGroup(iterator, 1, new double[]{4});
                expectStrTerm(iterator,"US", 5);
                expectGroup(iterator, 1, new double[]{5});

                expectEnd(iterator);
            }

            // GROUP BY q HAVING count() > 2, SELECT count()
            try (FTGAIterator iterator = multiFtgs(
                    Collections.singletonList(new SessionField(session, "q")),
                    Lists.newArrayList(count),
                    Lists.newArrayList(count.gt(constant(2.0))),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"デザイン", 4);
                expectGroup(iterator, 1, new double[]{4});

                expectEnd(iterator);
            }

            // GROUP BY q HAVING count() > 100, SELECT count()
            try (FTGAIterator iterator = multiFtgs(
                    Collections.singletonList(new SessionField(session, "q")),
                    Lists.newArrayList(count),
                    Lists.newArrayList(count.gt(constant(100.0))),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());
                assertFalse(iterator.nextTerm());
                expectEnd(iterator);
            }

            // GROUP BY q HAVING count() >= 2, SELECT count()
            try (FTGAIterator iterator = multiFtgs(
                    Collections.singletonList(new SessionField(session, "q")),
                    Lists.newArrayList(count),
                    Lists.newArrayList(count.gte(constant(2.0))),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"java", 2);
                expectGroup(iterator, 1, new double[]{2});

                expectStrTerm(iterator,"software engineer", 2);
                expectGroup(iterator, 1, new double[]{2});

                expectStrTerm(iterator,"デザイン", 4);
                expectGroup(iterator, 1, new double[]{4});

                expectEnd(iterator);
            }

            session.pushStat("clicks");
            session.pushStat("impressions");

            final AggregateStatTree clicks = stat(session, 1);
            final AggregateStatTree impressions = stat(session, 2);

            // GROUP BY q SELECT clicked / impressions
            // Also exercises having stats on the stack that aren't relevant.
            try (FTGAIterator iterator = multiFtgs(
                    Collections.singletonList(new SessionField(session, "q")),
                    Lists.newArrayList(clicks.divide(impressions)),
                    Lists.newArrayList(),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"entry level software engineer", 1);
                expectGroup(iterator, 1, new double[]{ 0.0 });

                expectStrTerm(iterator,"java", 2);
                expectGroup(iterator, 1, new double[]{ 0.20 });

                expectStrTerm(iterator,"java software engineer", 1);
                expectGroup(iterator, 1, new double[]{ 0.10 });

                expectStrTerm(iterator,"software engineer", 2);
                expectGroup(iterator, 1, new double[]{ 0.30 });

                expectStrTerm(iterator,"デザイン", 4);
                expectGroup(iterator, 1, new double[]{ 0.60 });

                expectEnd(iterator);
            }

            // GROUP BY q HAVING clicked / impressions >= 0.25 SELECT count()
            // Also exercises having stats on the stack that aren't relevant.
            try (FTGAIterator iterator = multiFtgs(
                    Collections.singletonList(new SessionField(session, "q")),
                    Lists.newArrayList(count),
                    Lists.newArrayList(clicks.divide(impressions).gte(constant(0.25))),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"software engineer", 2);
                expectGroup(iterator, 1, new double[]{ 2 });

                expectStrTerm(iterator,"デザイン", 4);
                expectGroup(iterator, 1, new double[]{ 4 });

                expectEnd(iterator);
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

        final Map<String, Long> countryDocFreqs = new HashMap<>();
        countryDocFreqs.put("AU", 10L);
        countryDocFreqs.put("GB", 1L);
        countryDocFreqs.put("JP", 70L);
        countryDocFreqs.put("US", 150L);

        for (int i = 0; i < numServers; i++) {
            clusterRunner.startDaemon();
        }

        try (ImhotepClient client = clusterRunner.createClient();
             ImhotepSession session1 = client.sessionBuilder(dataset1, TODAY.minusDays(2), TODAY).build();
             ImhotepSession session2 = client.sessionBuilder(dataset2, TODAY.minusDays(2), TODAY).build()
        ) {

            // GROUP BY country
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"AU", 10);
                expectGroup(iterator, 1, new double[]{});
                expectStrTerm(iterator,"GB", 1);
                expectGroup(iterator, 1, new double[]{});
                expectStrTerm(iterator,"JP", 70);
                expectGroup(iterator, 1, new double[]{});
                expectStrTerm(iterator,"US", 150);
                expectGroup(iterator, 1, new double[]{});

                expectEnd(iterator);
            }

            session1.pushStat("count()");
            session2.pushStat("count()");

            final AggregateStatTree count1 = stat(session1, 0);
            final AggregateStatTree count2 = stat(session2, 0);

            // GROUP BY country SELECT dataset1.count(), dataset2.count(), count()
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(count1, count2, count1.plus(count2)),
                    Lists.newArrayList(),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"AU", 10);
                expectGroup(iterator, 1, new double[]{10, 0, 10});
                expectStrTerm(iterator,"GB", 1);
                expectGroup(iterator, 1, new double[]{0, 1, 1});
                expectStrTerm(iterator,"JP", 70);
                expectGroup(iterator, 1, new double[]{20, 50, 70});
                expectStrTerm(iterator,"US", 150);
                expectGroup(iterator, 1, new double[]{140, 10, 150});

                expectEnd(iterator);
            }

            // GROUP BY country HAVING dataset1.count() > dataset2.count()
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(),
                    Lists.newArrayList(count1.gt(count2)),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"AU", 10);
                expectGroup(iterator, 1, new double[]{});
                expectStrTerm(iterator,"US", 150);
                expectGroup(iterator, 1, new double[]{});

                expectEnd(iterator);
            }

            // GROUP BY country LIMIT 1
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    false,
                    1,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                if (sortedFTGS) {
                    expectStrTerm(iterator, "AU", 10);
                } else {
                    // Can be any valid country if not expecting sorted.
                    assertTrue(iterator.nextTerm());
                    assertTrue(countryDocFreqs.containsKey(iterator.termStringVal()));
                    final long expectedTermDocFreq = countryDocFreqs.get(iterator.termStringVal());
                    assertEquals(expectedTermDocFreq, iterator.termDocFreq());
                }
                expectGroup(iterator, 1, new double[]{});

                expectEnd(iterator);
            }

            // GROUP BY country LIMIT 3
            try (FTGAIterator iterator = RemoteImhotepMultiSession.multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    false,
                    3,
                    -1,
                    sortedFTGS,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                if (sortedFTGS) {
                    expectStrTerm(iterator, "AU", 10);
                    expectGroup(iterator, 1, new double[]{});
                    expectStrTerm(iterator, "GB", 1);
                    expectGroup(iterator, 1, new double[]{});
                    expectStrTerm(iterator, "JP", 70);
                    expectGroup(iterator, 1, new double[]{});
                } else {
                    // Can be any valid country if not expecting sorted.

                    for (int i = 0; i < 3; i++) {
                        assertTrue(iterator.nextTerm());
                        assertTrue(countryDocFreqs.containsKey(iterator.termStringVal()));
                        final long expectedTermDocFreq = countryDocFreqs.get(iterator.termStringVal());
                        assertEquals(expectedTermDocFreq, iterator.termDocFreq());
                        expectGroup(iterator, 1, new double[]{});
                    }
                }

                expectEnd(iterator);
            }

            // GROUP BY country LIMIT 100
            try (FTGAIterator iterator = RemoteImhotepMultiSession.multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    false,
                    100,
                    -1,
                    sortedFTGS,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                if (sortedFTGS) {
                    expectStrTerm(iterator, "AU", 10);
                    expectGroup(iterator, 1, new double[]{});
                    expectStrTerm(iterator, "GB", 1);
                    expectGroup(iterator, 1, new double[]{});
                    expectStrTerm(iterator, "JP", 70);
                    expectGroup(iterator, 1, new double[]{});
                    expectStrTerm(iterator,"US", 150);
                    expectGroup(iterator, 1, new double[]{});
                } else {
                    // Can be any valid country if not expecting sorted.

                    for (int i = 0; i < 4; i++) {
                        assertTrue(iterator.nextTerm());
                        assertTrue(countryDocFreqs.containsKey(iterator.termStringVal()));
                        final long expectedTermDocFreq = countryDocFreqs.get(iterator.termStringVal());
                        assertEquals(expectedTermDocFreq, iterator.termDocFreq());
                        expectGroup(iterator, 1, new double[]{});
                    }
                }

                expectEnd(iterator);
            }

            // GROUP BY country HAVING count() > 10 LIMIT 1
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(),
                    Lists.newArrayList(count1.plus(count2).gt(constant(10.0))),
                    false,
                    1,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                if (sortedFTGS) {
                    expectStrTerm(iterator, "JP", 70);
                } else {
                    // Can be any valid country if not expecting sorted.
                    assertTrue(iterator.nextTerm());
                    assertTrue(countryDocFreqs.containsKey(iterator.termStringVal()));
                    final long expectedTermDocFreq = countryDocFreqs.get(iterator.termStringVal());
                    assertEquals(expectedTermDocFreq, iterator.termDocFreq());
                    assertTrue(expectedTermDocFreq > 10);
                }
                expectGroup(iterator, 1, new double[]{});

                expectEnd(iterator);
            }

            // GROUP BY country[1 BY count()]
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(count1.plus(count2)),
                    Lists.newArrayList(),
                    false,
                    1,
                    0,
                    StatsSortOrder.ASCENDING
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"US", 150);
                expectGroup(iterator, 1, new double[]{150});

                expectEnd(iterator);
            }

            // GROUP BY country[BOTTOM 1 BY count()]
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(count1.plus(count2)),
                    Lists.newArrayList(),
                    false,
                    1,
                    0,
                    StatsSortOrder.DESCENDING
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"GB", 1);
                expectGroup(iterator, 1, new double[]{1});

                expectEnd(iterator);
            }

            // GROUP BY country[1 BY dataset2.count()]
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(count2),
                    Lists.newArrayList(),
                    false,
                    1,
                    0,
                    StatsSortOrder.ASCENDING
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"JP", 70);
                expectGroup(iterator, 1, new double[]{50});

                expectEnd(iterator);
            }

            // GROUP BY country[BOTTOM 1 BY dataset2.count()]
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(count2),
                    Lists.newArrayList(),
                    false,
                    1,
                    0,
                    StatsSortOrder.DESCENDING
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"AU", 10);
                expectGroup(iterator, 1, new double[]{0});

                expectEnd(iterator);
            }

            // GROUP BY country[2 BY dataset1.count() - dataset2.count()] SELECT dataset1.count()
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(count1, count1.minus(count2)),
                    Lists.newArrayList(),
                    false,
                    2,
                    1,
                    StatsSortOrder.ASCENDING
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"AU", 10);
                expectGroup(iterator, 1, new double[]{10, 10});

                expectStrTerm(iterator,"US", 150);
                expectGroup(iterator, 1, new double[]{140, 130});

                expectEnd(iterator);
            }

            // GROUP BY country[1 BY count()] HAVING dataset1.count() <= 20
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(count1.plus(count2)),
                    Lists.newArrayList(count1.lte(constant(20))),
                    false,
                    1,
                    0,
                    StatsSortOrder.ASCENDING
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"JP", 70);
                expectGroup(iterator, 1, new double[]{70});

                expectEnd(iterator);
            }

            // GROUP BY country[BOTTOM 1 BY count()] HAVING dataset1.count() <= 20
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(count1.plus(count2)),
                    Lists.newArrayList(count1.lte(constant(20))),
                    false,
                    1,
                    0,
                    StatsSortOrder.DESCENDING
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"GB", 1);
                expectGroup(iterator, 1, new double[]{1});

                expectEnd(iterator);
            }

            // Imbalanced pushed stats
            session2.popStat();

            // GROUP BY country
            try (FTGAIterator iterator = multiFtgs(
                    ImmutableList.of(
                            new SessionField(session1, "country"),
                            new SessionField(session2, "country")
                    ),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    false,
                    0,
                    -1,
                    StatsSortOrder.UNDEFINED
            )) {
                assertTrue(iterator.nextField());
                assertEquals("magic", iterator.fieldName());
                assertFalse(iterator.fieldIsIntType());

                expectStrTerm(iterator,"AU", 10);
                expectGroup(iterator, 1, new double[]{});
                expectStrTerm(iterator,"GB", 1);
                expectGroup(iterator, 1, new double[]{});
                expectStrTerm(iterator,"JP", 70);
                expectGroup(iterator, 1, new double[]{});
                expectStrTerm(iterator,"US", 150);
                expectGroup(iterator, 1, new double[]{});

                expectEnd(iterator);
            }
        }
    }

    private FTGAIterator multiFtgs(
            final List<SessionField> sessionsWithFields,
            final List<AggregateStatTree> selects,
            final List<AggregateStatTree> filters,
            final boolean isIntField,
            final long termLimit,
            final int sortStat,
            final StatsSortOrder sortOrder
    ) {
        FTGAIterator iterator = RemoteImhotepMultiSession.multiFtgs(
                sessionsWithFields,
                selects,
                filters,
                isIntField,
                termLimit,
                sortStat,
                sortedFTGS,
                sortOrder
        );

        if (!sortedFTGS) {
            iterator = FTGSIteratorUtil.sortFTGSIterator(iterator);
        }

        return iterator;
    }

    private static String randomString(final Random rng, final StringBuilder sb, final int length) {
        sb.setLength(0);
        for (int i = 0; i < length; i++) {
            sb.append('a' + rng.nextInt(26));
        }
        return sb.toString();
    }

    @Test
    public void testUpstreamLimit() throws IOException, TimeoutException, InterruptedException, ImhotepOutOfMemoryException {
        final String limitDataset = "limit";
        final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();

        final Random rng = new Random(123L);
        final StringBuilder sb = new StringBuilder();
        final Object2IntOpenHashMap<String> termCounts = new Object2IntOpenHashMap<>();
        for (int i = 0; i < 1_000_000; i++) {
            // with 1M documents and a length of 4, the expected number of collisions for any given string is 2.2
            final String term = randomString(rng, sb, 4);
            termCounts.add(term, 1);
            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addStringTerm("field", term)
                    .build()
            );
        }

        final List<String> minimumTerms = termCounts.keySet().stream().sorted().limit(10).collect(Collectors.toList());

        clusterRunner.createDailyShard(limitDataset, TODAY.minusDays(1), memoryFlamdex);

        for (int i = 0; i < numServers; i++) {
            clusterRunner.startDaemon();
        }

        final ImhotepClient client = clusterRunner.createClient();

        final ImhotepClient.SessionBuilder sessionBuilder = client
                .sessionBuilder(limitDataset, TODAY.minusDays(1), TODAY)
                // 100 MB
                .daemonTempFileSizeLimit(100_000_000);

        try (final ImhotepSession session = sessionBuilder.build()) {
            // Try without limit to ensure that our ftgsMB is > 0
            try (FTGAIterator it = makeLimitIterator(session, 0)) {
            }

            final PerformanceStats performanceStats = session.closeAndGetPerformanceStats();
            // > 1 MB
            assertTrue(performanceStats.ftgsTempFileSize > 1_000_000);
        }

        try (final ImhotepSession session = sessionBuilder.build()) {
            try (final FTGAIterator ftga = makeLimitIterator(session, 10)) {
                final double[] statsBuf = new double[1];
                assertTrue(ftga.nextField());
                if (sortedFTGS) {
                    for (int i = 0; i < 10; i++) {
                        assertTrue(ftga.nextTerm());
                        final String term = ftga.termStringVal();
                        assertEquals(minimumTerms.get(i), term);
                        assertTrue(ftga.nextGroup());
                        ftga.groupStats(statsBuf);
                        assertEquals(termCounts.getInt(term), (int) statsBuf[0]);
                        assertFalse(ftga.nextGroup());
                    }
                } else {
                    for (int i = 0; i < 10; i++) {
                        assertTrue(ftga.nextTerm());
                        final String term = ftga.termStringVal();
                        assertTrue(termCounts.containsKey(term));
                        assertTrue(ftga.nextGroup());
                        ftga.groupStats(statsBuf);
                        assertEquals(termCounts.getInt(term), (int) statsBuf[0]);
                        assertFalse(ftga.nextGroup());
                    }
                }
                assertFalse(ftga.nextTerm());
                assertFalse(ftga.nextField());
            }

            final PerformanceStats performanceStats = session.closeAndGetPerformanceStats();
            // < 1KB
            assertTrue(performanceStats.ftgsTempFileSize < 1_000);
        }
    }

    private FTGAIterator makeLimitIterator(final ImhotepSession session, final int termLimit) throws ImhotepOutOfMemoryException {
        final AggregateStatTree count = AggregateStatTree.stat(session, session.pushStat("count()") - 1);
        return RemoteImhotepMultiSession.multiFtgs(
                ImmutableList.of(new SessionField(session, "field")),
                ImmutableList.of(count),
                ImmutableList.of(),
                false,
                termLimit,
                -1,
                sortedFTGS,
                StatsSortOrder.UNDEFINED
        );
    }
}
