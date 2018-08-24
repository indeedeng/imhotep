package com.indeed.imhotep;

import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import com.indeed.imhotep.service.ImhotepShardCreator;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestRemoteImhotepMultiSession {

    private ShardMasterAndImhotepDaemonClusterRunner clusterRunner;
    private Path storeDir;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        storeDir = Files.createTempDirectory("temp-imhotep");
        tempDir = Files.createTempDirectory("temp-imhotep");
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                storeDir.toFile(),
                tempDir.toFile(),
                ImhotepShardCreator.DEFAULT);
    }

    @After
    public void tearDown() throws IOException {
        clusterRunner.stop();
        FileUtils.deleteDirectory(tempDir.toFile());
        FileUtils.deleteDirectory(storeDir.toFile());
    }

    @Test
    public void testEmptyConditionsRegroup() throws IOException, TimeoutException, ImhotepOutOfMemoryException, InterruptedException {
        final String dataset = "dataset";
        final DateTime date = new DateTime(2018, 1, 1, 0, 0);
        final int duration = 10;
        final int docsPerShard = 1000;
        for (int i = 0; i < duration; i++) {
            clusterRunner.createDailyShard(dataset, date.plusDays(i), new MemoryFlamdex().setNumDocs(docsPerShard));
        }

        for (int i = 1; i < 5; i++) {
            clusterRunner.startDaemon();
        }

        final ImhotepClient client = clusterRunner.createClient();
        final ImhotepSession session = client.sessionBuilder(dataset, date, date.plusDays(duration)).build();
        final GroupMultiRemapMessage message =
                GroupMultiRemapMessage.newBuilder()
                        .setTargetGroup(1)
                        .setNegativeGroup(2)
                        .build();
        final int groupCount = session.regroupWithProtos(new GroupMultiRemapMessage[] {message}, true);
        assertEquals(3, groupCount);
        session.pushStat("count()");
        final long[] stats = session.getGroupStats(0);
        assertArrayEquals(new long[] {0, 0, duration * docsPerShard}, stats);
    }
}
