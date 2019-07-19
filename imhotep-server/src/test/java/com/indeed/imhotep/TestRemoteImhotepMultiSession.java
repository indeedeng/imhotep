package com.indeed.imhotep;

import com.google.common.collect.Lists;
import com.indeed.flamdex.MakeAFlamdex;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.service.ImhotepShardCreator;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
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
                storeDir,
                tempDir,
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
        final GroupMultiRemapRule rule = new GroupMultiRemapRule(1, 2, new int[0], new RegroupCondition[0]);
        final int groupCount = session.regroup(new GroupMultiRemapRule[] {rule}, true);
        assertEquals(3, groupCount);
        final long[] stats = session.getGroupStats(Collections.singletonList("count()"));
        assertArrayEquals(new long[] {0, 0, duration * docsPerShard}, stats);
    }

    @Test
    public void testUnconditionalRegroup() throws ImhotepOutOfMemoryException, IOException, TimeoutException, InterruptedException {

        final String dataset = "dataset";
        final DateTime date = new DateTime(2018, 1, 1, 0, 0);
        final int duration = 10;
        for (int i = 0; i < duration; i++) {
            final FlamdexReader reader = MakeAFlamdex.make();
            clusterRunner.createDailyShard(dataset, date.plusDays(i), reader);
        }

        for (int i = 1; i < 5; i++) {
            clusterRunner.startDaemon();
        }

        try (final ImhotepClient client = clusterRunner.createClient();
             final ImhotepSession session = client.sessionBuilder(dataset, date, date.plusDays(duration)).build()) {
            session.metricRegroup(Collections.singletonList("docId()"), (long) 0, session.getNumDocs(), (long) 1);
            final ArrayList<String> docIdPlusCount = Lists.newArrayList("docId()", "count()", "+");
            assertArrayEquals(new long[] {0,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200}, session.getGroupStats(docIdPlusCount));
            assertEquals(21, session.regroup(new int[] {10, 11, 12}, new int[]{0, 1, 2}, false));
            assertArrayEquals(new long[] {0,120,140,30,40,50,60,70,80,90,0,0,0,130,140,150,160,170,180,190,200}, session.getGroupStats(docIdPlusCount));
            assertEquals(5, session.regroup(new int[] {2, 6, 7}, new int[]{2, 3, 4}, true));
            assertArrayEquals(new long[] {0,0,140,60,70}, session.getGroupStats(docIdPlusCount));
        }
    }
}
