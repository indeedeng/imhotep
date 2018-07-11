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

import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

/**
 * @author aibragimov
 */

public class TestImhotepGetGroupStatsIterator {
    private static final Logger LOGGER = Logger.getLogger(TestImhotepGetGroupStatsIterator.class);
    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();

    private static final String DATASET = "dataset";

    @Rule
    public final TemporaryFolder rootDir = new TemporaryFolder();

    private ImhotepDaemonClusterRunner clusterRunner;

    @Before
    public void setUp() throws IOException {
        clusterRunner = new ImhotepDaemonClusterRunner(rootDir.newFolder("shards"), rootDir.newFolder("temp"));
    }

    @After
    public void tearDown() throws IOException, TimeoutException {
        clusterRunner.stop();
    }

    private Pair<MemoryFlamdex[], GroupMultiRemapRule> createTestData(final int shardsCount, final int docsCount) {

        final MemoryFlamdex[] memoryDocs = new MemoryFlamdex[shardsCount];
        for (int i = 0; i < memoryDocs.length; i++) {
            memoryDocs[i] = new MemoryFlamdex();
        }

        final RegroupCondition[] conditions = new RegroupCondition[docsCount];
        final int[] index = new int[docsCount];

        for (int i = 0; i < docsCount; i++) {
            for (int shardIndex = 0; shardIndex < memoryDocs.length; shardIndex++) {
                memoryDocs[shardIndex].addDocument(new FlamdexDocument.Builder()
                        .addIntTerm("shardId", shardIndex)
                        .addIntTerm("id", i)
                        .build());
            }
            conditions[i] = new RegroupCondition("id", true, i, "", false);
            index[i] = i;
        }

        final GroupMultiRemapRule rule = new GroupMultiRemapRule(1, 0, index, conditions);

        return Pair.of(memoryDocs, rule);
    }

    private void runTest(final int shardCount, final int docCount, final int daemonCount)
            throws IOException, ImhotepOutOfMemoryException, TimeoutException {

        final Pair<MemoryFlamdex[], GroupMultiRemapRule> data = createTestData(shardCount, docCount);

        for (int i = 0; i < data.getFirst().length; i++) {
            clusterRunner.createDailyShard(DATASET, TODAY.minusDays(1 + i), data.getFirst()[i]);
        }

        for (int i = 0; i < daemonCount; i++) {
            clusterRunner.startDaemon();
        }

        try(
                final ImhotepClient client = clusterRunner.createClient();
                final ImhotepSession dataset = client.sessionBuilder(DATASET, TODAY.minusDays(1 + data.getFirst().length), TODAY).build()
        ) {
            dataset.regroup(new GroupMultiRemapRule[]{data.getSecond()});

            dataset.pushStat("id");
            dataset.pushStat("shardId");

            final long[] idSum = dataset.getGroupStats(0);
            final long[] shardIdSum = dataset.getGroupStats(1);

            assertEquals(idSum.length, docCount);
            assertEquals(shardIdSum.length, docCount);

            assertEquals(idSum[0], 0);
            for (int i = 1; i < idSum.length; i++) {
                assertEquals(idSum[i], i * shardCount);
            }

            // sum [0..shardCount-1]
            final int expectedShardIdSum = shardCount * (shardCount - 1) / 2;
            assertEquals(shardIdSum[0], 0);
            for (int i = 1; i < shardIdSum.length; i++) {
                assertEquals(shardIdSum[i], expectedShardIdSum);
            }
        }
    }

    @Test
    public void testSingleSession() throws IOException, ImhotepOutOfMemoryException, InterruptedException, TimeoutException {
        runTest(5, 1000, 1);
    }

    @Test
    public void testMultiSession() throws IOException, ImhotepOutOfMemoryException, InterruptedException, TimeoutException {
        runTest(10, 10000, 5);
    }

    @Test
    public void testIMTEPD400() throws IOException, TimeoutException, ImhotepOutOfMemoryException, InterruptedException {
        final Pair<MemoryFlamdex[], GroupMultiRemapRule> data = createTestData(5, 1000);

        for (int i = 0; i < data.getFirst().length; i++) {
            clusterRunner.createDailyShard(DATASET, TODAY.minusDays(1 + i), data.getFirst()[i]);
        }

        for (int i = 0; i < 5; i++) {
            clusterRunner.startDaemon();
        }

        try(
            final ImhotepClient client = clusterRunner.createClient();
            final ImhotepSession dataset = client.sessionBuilder(DATASET, TODAY.minusDays(1 + data.getFirst().length), TODAY).build()
        ) {
            // remapping all docs to group 1.000.000 to have big group stats array
            dataset.intOrRegroup("fakeField", new long[0], 1, 1000000, 1000000);
            dataset.pushStat("count()");
            final GroupStatsIterator iterator = dataset.getGroupStatsIterator(0);
            iterator.close(); // in IMTEPD-400 daemons are closing sessions here.

            // Wait a little bit, let daemons to fail.
            Thread.sleep(1000);

            // Check, if session is still alive
            assertEquals(2, dataset.pushStat("count()"));
        }
    }
}
