/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.client;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.service.ImhotepDaemonRunner;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jsgroth
 */
public class TestImhotepClient {

    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    private static final String SHARD0 = "index20130418.18-20130418.19";
    private static final String SHARD1 = "index20130418.19-20130418.20";
    private static final String DATASET = "dataset";

    private ImhotepDaemonRunner daemon1;
    private ImhotepDaemonRunner daemon2;

    @Before
    public void setUp() throws Exception {
        final Path tempDir1 = tempDir.newFolder("test1").toPath();
        final Path tempOptDir1 = tempDir.newFolder("optimized.test1").toPath();
        final Path datasetDir = tempDir1.resolve(DATASET);
        Files.createDirectories(datasetDir);
        Files.createDirectories(datasetDir.resolve(SHARD0));
        Files.createDirectories(datasetDir.resolve(SHARD1));

        final Path tempDir2 = tempDir.newFolder("test2").toPath();
        final Path tempOptDir2 = tempDir.newFolder("optimized.test2").toPath();
        final Path datasetDir2 = tempDir2.resolve(DATASET);
        Files.createDirectories(datasetDir2);
        Files.createDirectories(datasetDir2.resolve(SHARD1));

        daemon1 = new ImhotepDaemonRunner(tempDir1, tempOptDir1, getFreePort());
        daemon2 = new ImhotepDaemonRunner(tempDir2, tempOptDir2, getFreePort());
    }

    private static int getFreePort() throws IOException {
        try(ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    @After
    public void tearDown() throws IOException {
        if (daemon1 != null) {
            daemon1.stop();
        }
        if (daemon2 != null) {
            daemon2.stop();
        }
    }

    @Test
    public void testFailure() throws Exception {
        daemon1.start();
        daemon2.start();
        final ImhotepClient client = new ImhotepClient(Arrays.asList(new Host("localhost", daemon1.getPort()), new Host("localhost", daemon2.getPort())));
        daemon2.stop();
        ImhotepSession session = client.sessionBuilder(DATASET, null, null).shardsOverride(Arrays.asList(SHARD0, SHARD1)).build();
        session.close();

        session = client.sessionBuilder(DATASET, new DateTime(2013, 4, 18, 0, 0), new DateTime(2013, 4, 19, 0, 0)).build();
        session.close();

        client.close();
        daemon1.stop();
    }

    @Test
    public void testRealFailure() throws Exception {
        daemon1.stop();
        final ImhotepClient client = new ImhotepClient(Collections.singletonList(new Host("localhost", daemon1.getPort())));
        try {
            client.sessionBuilder(DATASET, null, null).shardsOverride(Collections.singletonList(SHARD0)).build();
            fail("session opening did not fail when it should have");
        } catch (final RuntimeException e) {
            // pass
        }

        try {
            client.sessionBuilder(DATASET, new DateTime(2013, 4, 18, 18, 0), new DateTime(2013, 4, 18, 19, 0)).build();
            fail("session opening did not fail when it should have");
        } catch (final RuntimeException e) {
            // pass
        }
        client.close();
    }


    @Test
    public void testRemoveIntersectingShards() {
        final List<String> largerShardOlder = Lists.newArrayList("index20130418.18-20130418.21.20030101000000");
        final List<String> splitShards = Lists.newArrayList(
                "index20130418.18-20130418.19.20130101000000",
                "index20130418.19-20130418.20.20130101000000",
                "index20130418.20-20130418.21.20130101000000");
        List<String> shardIds = Lists.newArrayList(Iterables.concat(largerShardOlder, splitShards));
        List<String> expectedShards = splitShards;
        removeIntersecingShardsHelper(shardIds, expectedShards);

        final List<String> largerShardNewer = Lists.newArrayList("index20130418.18-20130418.21.20140101000000");
        shardIds = Lists.newArrayList(Iterables.concat(splitShards, largerShardNewer));
        expectedShards = largerShardNewer;
        removeIntersecingShardsHelper(shardIds, expectedShards);
    }

    @Test
    public void testRemoveIntersectingShardsPartialIntersect() {
        final String largerShardOlder = "index20130418.00-20130419.00.20130101000000";
        final String smallerShardNewer = "index20130418.18-20130418.19.20140101000000";
        final List<String> shardIds = Lists.newArrayList(largerShardOlder, smallerShardNewer);
        final List<String> expectedShards = Lists.newArrayList(smallerShardNewer);
        removeIntersecingShardsHelper(shardIds, expectedShards, new DateTime(2013, 4, 18, 18, 0));
    }

    private void removeIntersecingShardsHelper(final List<String> shardIds, final List<String> expectedShards) {
        final DateTime start = new DateTime(2000, 1, 1, 0, 0);
        removeIntersecingShardsHelper(shardIds, expectedShards, start);
    }
    private void removeIntersecingShardsHelper(final List<String> shardIds, List<String> expectedShards, final DateTime start) {
        final List<ShardIdWithVersion> shards = shardBuilder(shardIds);
        expectedShards = stripVersions(expectedShards);
        final List<ShardIdWithVersion> result = ImhotepClient.removeIntersectingShards(shards, "test", start);

        final String noMatchMsg = "chosen shard list doesn't match the expected." +
                "\nChosen: " + Arrays.toString(result.toArray()) +
                "\nExpected: " + Arrays.toString(expectedShards.toArray());

        assertEquals(noMatchMsg, result.size(), expectedShards.size());

        for(final ShardIdWithVersion shard : result) {
            assertTrue(noMatchMsg, expectedShards.contains(shard.getShardId()));
        }
    }

    private static List<String> stripVersions(final List<String> shardIds) {
        final List<String> stripped = Lists.newArrayList();
        for(String shardId : shardIds) {
            if(shardId.length() > 28) {
                final int dotIndex = shardId.lastIndexOf('.');
                shardId = shardId.substring(0, dotIndex);
            }
            stripped.add(shardId);
        }
        return stripped;
    }

    private static List<ShardIdWithVersion> shardBuilder(final List<String> shardIds) {
        final List<ShardIdWithVersion> shards = Lists.newArrayList();
        for(String shardId : shardIds) {
            long version = 0;
            if(shardId.length() > 28) {
                final int dotIndex = shardId.lastIndexOf('.');
                final String versionStr = shardId.substring(dotIndex + 1);
                version = Long.parseLong(versionStr);
                shardId = shardId.substring(0, dotIndex);
            }
            shards.add(new ShardIdWithVersion(shardId, version));
        }
        return shards;
    }
}
