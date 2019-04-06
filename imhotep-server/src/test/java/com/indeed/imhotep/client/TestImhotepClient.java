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
 package com.indeed.imhotep.client;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardDir;
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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

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
    public void setUp() throws IOException, TimeoutException {
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

    public static int getFreePort() throws IOException {
        try (final ServerSocket ss = new ServerSocket(0)) {
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

    /* TODO: this test fails now as we don't auto-retry updating the shard list on the first session opening failure
    @Test
    public void testFailure() throws IOException, TimeoutException {
        daemon1.start();
        daemon2.start();
        Host host1 = new Host("localhost", daemon1.getActualPort());
        Host host2 = new Host("localhost", daemon2.getActualPort());

        final ImhotepClient client = new ImhotepClient(Arrays.asList(host1, host2));
        daemon2.stop();
        LocatedShardInfo shard0 = new LocatedShardInfo(SHARD0, 0, 0);
        LocatedShardInfo shard1 = new LocatedShardInfo(SHARD1, 0, 0);
        shard0.getServers().add(host1);
        shard1.getServers().add(host1);
        shard1.getServers().add(host2);

        ImhotepSession session = client.sessionBuilder(DATASET, null, null).shardsOverride(Arrays.asList(shard0, shard1)).build();
        session.close();

        session = client.sessionBuilder(DATASET, new DateTime(2013, 4, 18, 0, 0), new DateTime(2013, 4, 19, 0, 0)).build();
        session.close();

        client.close();
        daemon1.stop();
    }*/

    @Test
    public void testRealFailure() throws IOException {
        daemon1.stop();
        final Host host1 = new Host("localhost", daemon1.getPort());
        try (final ImhotepClient client = new ImhotepClient(Collections.singletonList(host1))) {
            final Shard shard0 = new Shard(SHARD0, 0, 0, host1);
            client.sessionBuilder(DATASET, null, null).shardsOverride(Collections.singletonList(shard0)).build();
            fail("session opening did not fail when it should have");
        } catch (final RuntimeException e) {
            // pass
        }
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

    @Test
    public void testRemoveIntersectingShardsDynamicShards() {
        final String shard1subshard1 = "dindex20130418.00-20130419.00.0.2.10.10";
        final String shard1subshard2 = "dindex20130418.00-20130419.00.1.2.10.10";
        final String shard2subshard1 = "dindex20130419.01-20130420.00.0.2.10.10";
        final String shard2subshard2older = "dindex20130419.01-20130420.00.1.2.10.10";
        final String shard2subshard2newer = "dindex20130419.01-20130420.00.1.2.1000.10";
        final List<String> shardIds = Lists.newArrayList(shard1subshard1, shard1subshard2, shard2subshard1, shard2subshard2older, shard2subshard2newer);
        final List<String> expectedShards = Lists.newArrayList(shard1subshard1, shard1subshard2, shard2subshard1, shard2subshard2newer);
        removeIntersecingShardsHelper(shardIds, expectedShards);
    }

    private void removeIntersecingShardsHelper(final List<String> shardIds, final List<String> expectedShards) {
        final DateTime start = new DateTime(2000, 1, 1, 0, 0);
        removeIntersecingShardsHelper(shardIds, expectedShards, start);
    }

    private void removeIntersecingShardsHelper(
            final List<String> shardIds,
            List<String> expectedShards,
            final DateTime start) {
        final List<Shard> shards = shardBuilder(shardIds);
        expectedShards = stripVersions(expectedShards);
        final List<Shard> result = ImhotepClient.removeIntersectingShards(shards, "test", start);

        final String noMatchMsg = "chosen shard list doesn't match the expected." +
                "\nChosen: " + Arrays.toString(result.toArray()) +
                "\nExpected: " + Arrays.toString(expectedShards.toArray());

        assertEquals(noMatchMsg, result.size(), expectedShards.size());

        for(final Shard shard : result) {
            assertTrue(noMatchMsg, expectedShards.contains(shard.getShardId()));
        }
    }

    private static List<String> stripVersions(final List<String> shardIds) {
        final List<String> stripped = Lists.newArrayList();
        for (final String shardId : shardIds) {
            final ShardDir shardDir = new ShardDir(Paths.get("/datasetName").resolve(shardId));
            stripped.add(shardDir.getId());
        }
        return stripped;
    }

    private static List<Shard> shardBuilder(final List<String> shardIds) {
        final List<Shard> shards = Lists.newArrayList();
        for (final String shardId : shardIds) {
            final ShardDir shardDir = new ShardDir(Paths.get("/datasetName").resolve(shardId));
            shards.add(new Shard(shardDir.getId(), 0, shardDir.getVersion(), null));
        }
        return shards;
    }
}
