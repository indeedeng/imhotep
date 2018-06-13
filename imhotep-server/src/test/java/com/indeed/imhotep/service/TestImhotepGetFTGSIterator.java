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
import com.indeed.imhotep.FTGSIteratorTestUtils;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author kenh
 */

@RunWith(Parameterized.class)
public class TestImhotepGetFTGSIterator {
    private static final Logger LOGGER = Logger.getLogger(TestImhotepGetFTGSIterator.class);
    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();

    private static final String DATASET = "dataset";

    @Parameterized.Parameters
    public static List<Boolean[]> isSortedFTGS() {
        return Lists.newArrayList(new Boolean[] {true }, new Boolean[] {false});
    }

    private final boolean sortedFTGS;

    public TestImhotepGetFTGSIterator(final Boolean sortedFTGS) {
        this.sortedFTGS = sortedFTGS;
    }

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

    @Test
    public void testSingleSession() throws IOException, ImhotepOutOfMemoryException, InterruptedException, TimeoutException {
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

        clusterRunner.createDailyShard(DATASET, TODAY.minusDays(1), memoryFlamdex);
        clusterRunner.startDaemon();

        try (
                final ImhotepClient client = clusterRunner.createClient();
                final ImhotepSession dataset = client.sessionBuilder(DATASET, TODAY.minusDays(1), TODAY).build()
        ) {
            // get full FTGS
            {
                final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"});

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            // get first 12 terms
            {
                final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 12);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            dataset.pushStat("metric");

            // get full FTGS with pushed stats (100 terms is enough to capture all)
            for (final FTGSIterator iter : Arrays.asList(
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
            )) {
                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            // get top 2 terms per field for the only group stat
            {
                final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 0);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            dataset.pushStat("metric2");

            // get full FTGS with pushed stats (100 terms is enough to capture all)
            for (final FTGSIterator iter : Arrays.asList(
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
            )) {
                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            // get top 2 terms per field for the second group stat
            {
                final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            // map all documents with 'metric = 3' to group 2
            dataset.regroup(new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{new RegroupCondition("metric", true, 3, null, false)})
            });

            // get full FTGS with pushed stats (100 terms is enough to capture all)
            for (final FTGSIterator iter : Arrays.asList(
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
            )) {
                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            // get top 2 terms per field, per group for the second group stat
            {
                final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectEnd(iter);
            }
        }
    }

    @Test
    public void testMultiSession() throws IOException, ImhotepOutOfMemoryException, InterruptedException, TimeoutException {

        {
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

            clusterRunner.createDailyShard(DATASET, TODAY.minusDays(2), memoryFlamdex);
        }

        {
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

            clusterRunner.createDailyShard(DATASET, TODAY.minusDays(1), memoryFlamdex);
        }

        clusterRunner.startDaemon();
        clusterRunner.startDaemon();

        try (
                final ImhotepClient client = clusterRunner.createClient();
                final ImhotepSession dataset = client.sessionBuilder(DATASET, TODAY.minusDays(2), TODAY).build()
        ) {
            // get first 15 terms
            // different prosessing for sorted and unsorted
            if (sortedFTGS) {
                final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                        getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 15),
                        1);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrField(iter, "sf2");

                FTGSIteratorTestUtils.expectEnd(iter);
            } else {
                // Not using helper method here since we want to check actual order of fields
                final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                        dataset.getFTGSIterator(
                                new FTGSParams(
                                        new String[]{"if1", "if2", "metric"},
                                        new String[]{"sf1", "sf2"}, 15, -1, false)),
                        1);

                // Maybe we need better testing for unsorted here
                // Now we just check that fields are in the same order,
                // result have exactly 15 terms
                // and all terms have one group(1) and same stats
                int termsCount = 0;
                final long[] stats = new long[1];
                for (final String field : new String[]{"if1", "if2", "metric", "sf1", "sf2"}) {
                    assertTrue(iter.nextField());
                    assertEquals(iter.fieldName(), field);
                    assertEquals(iter.fieldIsIntType(), !field.startsWith("sf"));
                    while(iter.nextTerm()) {
                        termsCount++;
                        assertTrue(iter.nextGroup());
                        assertEquals(1, iter.group());
                        iter.groupStats(stats);
                        assertArrayEquals(stats, new long[]{0});
                        assertFalse(iter.nextGroup());
                    }
                }
                assertFalse(iter.nextField());
                assertEquals(15, termsCount);
            }

            // get full FTGS
            {
                final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                        getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}), 1);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
                FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            dataset.pushStat("metric");

            // get full FTGS with pushed stats (100 terms is enough to capture all)
            for (final FTGSIterator iterator : Arrays.asList(
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
            )) {
                final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                        iterator, 1);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});
                FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
                FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
                FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
                FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            // get top 4 terms per field for the only group stat
            {
                final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                        getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 4, 0), 1);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});
                FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
                FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
                FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
                FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            dataset.pushStat("metric2");

            // get full FTGS with pushed stats (100 terms is enough to capture all)
            for (final FTGSIterator iterator : Arrays.asList(
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
            )) {
                final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                        iterator, 2);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});
                FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21, -21});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
                FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21, -21});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            // get top 2 terms per field for the second group stat
            {
                final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                        getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1), 2);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21, -21});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21, -21});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            // map all documents with 'metric = 3' to group 2
            dataset.regroup(new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{new RegroupCondition("metric", true, 3, null, false)}),
            });

            // get full FTGS with pushed stats (100 terms is enough to capture all)
            for (final FTGSIterator iterator : Arrays.asList(
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
            )) {
                final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                        iterator, 2);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});
                FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{18, -18});
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
                FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
                FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
                FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{18, -18});
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            // get top 4 terms per field, per group for the second group stat
            {
                final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                        getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 4, 1), 2);

                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{18, -18});
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
                FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
                FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{18, -18});
                FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

                FTGSIteratorTestUtils.expectEnd(iter);
            }
        }
    }

    private FTGSIterator getFTGSIterator(final ImhotepSession session,
                                         final String[] intFields,
                                         final String[] stringFields) {
        return getFTGSIterator(session, intFields, stringFields, 0);
    }

    private FTGSIterator getFTGSIterator(final ImhotepSession session,
                                         final String[] intFields,
                                         final String[] stringFields,
                                         final long termLimit) {
        return getFTGSIterator(session, intFields, stringFields, termLimit, -1);
    }

    private FTGSIterator getFTGSIterator(final ImhotepSession session,
                                         final String[] intFields,
                                         final String[] stringFields,
                                         final long termLimit,
                                         final int sortStat) {
        FTGSIterator iterator = session.getFTGSIterator(new FTGSParams(intFields, stringFields, termLimit, sortStat, sortedFTGS));
        if (!sortedFTGS) {
            iterator = FTGSIteratorUtil.getTopTermsFTGSIterator(iterator, Long.MAX_VALUE, session.getNumStats(), -1);
        }
        return iterator;
    }
}
