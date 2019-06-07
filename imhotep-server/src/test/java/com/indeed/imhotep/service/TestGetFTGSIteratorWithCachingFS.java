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
import com.indeed.imhotep.fs.RemoteCachingFileSystemTestContext;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singletonList;

/**
 * @author kenh
 */

@RunWith(Parameterized.class)
public class TestGetFTGSIteratorWithCachingFS {
    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();

    private static final String DATASET = "dataset";

    @Rule
    public final RemoteCachingFileSystemTestContext fsTestContext = new RemoteCachingFileSystemTestContext();

    private ShardMasterAndImhotepDaemonClusterRunner clusterRunner;

    @Parameterized.Parameters
    public static List<Boolean[]> isSortedFTGS() {
        return Lists.newArrayList(new Boolean[] {true }, new Boolean[] {false});
    }

    private final boolean sortedFTGS;

    public TestGetFTGSIteratorWithCachingFS(final Boolean sortedFTGS) {
        this.sortedFTGS = sortedFTGS;
    }

    @Before
    public void setUp() throws IOException, URISyntaxException {
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                fsTestContext.getLocalStoreDir().toPath(),
                fsTestContext.getTempRootDir().toPath(),
                ImhotepShardCreator.GZIP_ARCHIVE);
    }

    @After
    public void tearDown() throws IOException, TimeoutException {
        clusterRunner.stop();
    }

    @Test
    public void testSingleSession() throws IOException, ImhotepOutOfMemoryException, InterruptedException, TimeoutException, URISyntaxException {
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

        // start daemons with imhotep filesystem
        clusterRunner.startDaemon(Paths.get(new URI("imhtpfs:///")));

        try (
                final ImhotepClient client = clusterRunner.createClient();
                final ImhotepSession dataset = client.sessionBuilder(DATASET, TODAY.minusDays(1), TODAY).build()
        ) {
            final List<List<String>> stats = new ArrayList<>();

            // get full FTGS
            try (final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, stats);) {
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
            try (final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 12, stats);) {
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

            stats.add(singletonList("metric"));

            // get full FTGS with pushed stats (100 terms is enough to capture all)
            for (final FTGSIterator iter : Arrays.asList(
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, stats),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, stats),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0, stats, StatsSortOrder.ASCENDING),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0, stats, StatsSortOrder.DESCENDING)
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
                iter.close();
            }

            // get top 2 terms per field for the only group stat
            try (final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 0, stats, StatsSortOrder.ASCENDING)) {
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

            // get bottom 2 terms per field for the only group stat
            try (final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 0, stats, StatsSortOrder.DESCENDING)) {
                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});

                FTGSIteratorTestUtils.expectStrField(iter, "sf2");
                FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

                FTGSIteratorTestUtils.expectEnd(iter);
            }

            stats.add(singletonList("metric2"));

            // get full FTGS with pushed stats (100 terms is enough to capture all)
            for (final FTGSIterator iter : Arrays.asList(
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, stats),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, stats),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0, stats, StatsSortOrder.ASCENDING),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0, stats, StatsSortOrder.DESCENDING)
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
                iter.close();
            }

            // get top 2 terms per field for the second group stat
            try (final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1, stats, StatsSortOrder.ASCENDING)) {
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

            // get bottom 2 terms per field for the second group stat
            try (final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1, stats, StatsSortOrder.DESCENDING)) {
                FTGSIteratorTestUtils.expectIntField(iter, "if1");
                FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
                FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});

                FTGSIteratorTestUtils.expectIntField(iter, "if2");
                FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

                FTGSIteratorTestUtils.expectIntField(iter, "metric");
                FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});

                FTGSIteratorTestUtils.expectStrField(iter, "sf1");
                FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
                FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
                FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});

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
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, stats),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, stats),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0, stats, StatsSortOrder.ASCENDING),
                    getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0, stats, StatsSortOrder.DESCENDING)
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
                iter.close();
            }

            // get top 2 terms per field, per group for the second group stat
            try (final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1, stats, StatsSortOrder.ASCENDING)) {
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

            // get bottom 2 terms per field, per group for the second group stat
            try (final FTGSIterator iter = getFTGSIterator(dataset, new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1, stats, StatsSortOrder.DESCENDING)) {
                FTGSIteratorTestUtils.expectIntField(iter, "if1");
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
        }
    }

    private FTGSIterator getFTGSIterator(final ImhotepSession session,
                                         final String[] intFields,
                                         final String[] stringFields,
                                         final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return getFTGSIterator(session, intFields, stringFields, 0, stats);
    }

    private FTGSIterator getFTGSIterator(final ImhotepSession session,
                                         final String[] intFields,
                                         final String[] stringFields,
                                         final long termLimit,
                                         final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return getFTGSIterator(session, intFields, stringFields, termLimit, -1, stats, StatsSortOrder.UNDEFINED);
    }

    private FTGSIterator getFTGSIterator(final ImhotepSession session,
                                         final String[] intFields,
                                         final String[] stringFields,
                                         final long termLimit,
                                         final int sortStat,
                                         final List<List<String>> stats,
                                         final StatsSortOrder statsSortOrder) throws ImhotepOutOfMemoryException {
        FTGSIterator iterator = session.getFTGSIterator(new FTGSParams(intFields, stringFields, termLimit, sortStat, sortedFTGS, stats, statsSortOrder));
        if (!sortedFTGS) {
            iterator = FTGSIteratorUtil.sortFTGSIterator(iterator);
        }
        return iterator;
    }
}
