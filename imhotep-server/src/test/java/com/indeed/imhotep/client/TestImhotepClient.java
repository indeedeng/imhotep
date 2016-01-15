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
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.util.io.Files;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.service.ImhotepDaemonRunner;
import junit.framework.TestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestImhotepClient extends TestCase {

    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }
    private String tempDir1;
    private String tempOptDir1;
    private String tempDir2;
    private String tempOptDir2;

    private static final String SHARD0 = "index20130418.18-20130418.19";
    private static final String SHARD1 = "index20130418.19-20130418.20";
    private static final String DATASET = "dataset";

    private ImhotepDaemonRunner daemon1;
    private ImhotepDaemonRunner daemon2;

    @Override
    protected void setUp() throws Exception {
        tempDir1 = Files.getTempDirectory("imhotep", "test");
        tempOptDir1 = Files.getTempDirectory("imhotep", "optimize.test");
        String datasetDir = new File(tempDir1, DATASET).getAbsolutePath();
        new File(datasetDir).mkdir();
        new File(datasetDir, SHARD0).mkdir();
        new File(datasetDir, SHARD1).mkdir();

        tempDir2 = Files.getTempDirectory("imhotep", "test");
        tempOptDir2 = Files.getTempDirectory("imhotep", "optimize.test");
        String datasetDir2 = new File(tempDir2, DATASET).getAbsolutePath();
        new File(datasetDir2).mkdir();
        new File(datasetDir2, SHARD1).mkdir();

        daemon1 = new ImhotepDaemonRunner(tempDir1, tempOptDir1, getFreePort());
        daemon2 = new ImhotepDaemonRunner(tempDir2, tempOptDir2, getFreePort());
    }

    private static int getFreePort() throws IOException {
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        ss.close();
        return port;
    }

    @Override
    protected void tearDown() throws Exception {
        if (daemon1 != null) {
            daemon1.stop();
        }
        if (daemon2 != null) {
            daemon2.stop();
        }
        Files.delete(tempDir1);
        Files.delete(tempOptDir1);
        Files.delete(tempDir2);
        Files.delete(tempOptDir2);
    }

    @Test
    public void testFailure() throws Exception {
        daemon1.start();
        daemon2.start();
        ImhotepClient client = new ImhotepClient(Arrays.asList(new Host("localhost", daemon1.getPort()), new Host("localhost", daemon2.getPort())));
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
        ImhotepClient client = new ImhotepClient(Arrays.asList(new Host("localhost", daemon1.getPort())));
        try {
            client.sessionBuilder(DATASET, null, null).shardsOverride(Arrays.asList(SHARD0)).build();
            fail("session opening did not fail when it should have");
        } catch (RuntimeException e) {
            // pass
        }

        try {
            client.sessionBuilder(DATASET, new DateTime(2013, 4, 18, 18, 0), new DateTime(2013, 4, 18, 19, 0)).build();
            fail("session opening did not fail when it should have");
        } catch (RuntimeException e) {
            // pass
        }
        client.close();
    }


    @Test
    public void testRemoveIntersectingShards() {
        List<String> largerShardOlder = Lists.newArrayList("index20130418.18-20130418.21.20030101000000");
        List<String> splitShards = Lists.newArrayList(
                "index20130418.18-20130418.19.20130101000000",
                "index20130418.19-20130418.20.20130101000000",
                "index20130418.20-20130418.21.20130101000000");
        List<String> shardIds = Lists.newArrayList(Iterables.concat(largerShardOlder, splitShards));
        List<String> expectedShards = splitShards;
        removeIntersecingShardsHelper(shardIds, expectedShards);

        List<String> largerShardNewer = Lists.newArrayList("index20130418.18-20130418.21.20140101000000");
        shardIds = Lists.newArrayList(Iterables.concat(splitShards, largerShardNewer));
        expectedShards = largerShardNewer;
        removeIntersecingShardsHelper(shardIds, expectedShards);
    }

    @Test
    public void testRemoveIntersectingShardsPartialIntersect() {
        String largerShardOlder = "index20130418.00-20130419.00.20130101000000";
        String smallerShardNewer = "index20130418.18-20130418.19.20140101000000";
        List<String> shardIds = Lists.newArrayList(largerShardOlder, smallerShardNewer);
        List<String> expectedShards = Lists.newArrayList(smallerShardNewer);
        removeIntersecingShardsHelper(shardIds, expectedShards, new DateTime(2013, 4, 18, 18, 0));
    }

    private void removeIntersecingShardsHelper(List<String> shardIds, List<String> expectedShards) {
        DateTime start = new DateTime(2000, 1, 1, 0, 0);
        removeIntersecingShardsHelper(shardIds, expectedShards, start);
    }
    private void removeIntersecingShardsHelper(List<String> shardIds, List<String> expectedShards, DateTime start) {
        List<ShardIdWithVersion> shards = shardBuilder(shardIds);
        expectedShards = stripVersions(expectedShards);
        List<ShardIdWithVersion> result = ImhotepClient.removeIntersectingShards(shards, "test", start);

        String noMatchMsg = "chosen shard list doesn't match the expected." +
                "\nChosen: " + Arrays.toString(result.toArray()) +
                "\nExpected: " + Arrays.toString(expectedShards.toArray());

        assertTrue(noMatchMsg, result.size() == expectedShards.size());

        for(ShardIdWithVersion shard : result) {
            assertTrue(noMatchMsg, expectedShards.contains(shard.getShardId()));
        }
    }

    private static List<String> stripVersions(List<String> shardIds) {
        List<String> stripped = Lists.newArrayList();
        for(String shardId : shardIds) {
            if(shardId.length() > 28) {
                int dotIndex = shardId.lastIndexOf('.');
                shardId = shardId.substring(0, dotIndex);
            }
            stripped.add(shardId);
        }
        return stripped;
    }

    private static List<ShardIdWithVersion> shardBuilder(List<String> shardIds) {
        List<ShardIdWithVersion> shards = Lists.newArrayList();
        for(String shardId : shardIds) {
            long version = 0;
            if(shardId.length() > 28) {
                int dotIndex = shardId.lastIndexOf('.');
                String versionStr = shardId.substring(dotIndex + 1);
                version = Long.parseLong(versionStr);
                shardId = shardId.substring(0, dotIndex);
            }
            shards.add(new ShardIdWithVersion(shardId, version));
        }
        return shards;
    }
}
