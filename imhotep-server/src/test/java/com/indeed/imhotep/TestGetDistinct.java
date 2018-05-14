package com.indeed.imhotep;

import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.service.ImhotepDaemonClusterRunner;
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
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestGetDistinct {

    private ImhotepDaemonClusterRunner clusterRunner;
    private Path storeDir;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        storeDir = Files.createTempDirectory("temp-imhotep");
        tempDir = Files.createTempDirectory("temp-imhotep");
        clusterRunner = new ImhotepDaemonClusterRunner(
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
    public void testIntField() throws IOException, TimeoutException {
        final String dataset = "dataset";
        final DateTime date = new DateTime(2018, 1, 1, 0, 0);
        final int duration = 10;
        for (int i = 0; i < duration; i++) {
            clusterRunner.createDailyShard(dataset, date.plusDays(i), createReader(i+1));
        }

        for (int i = 1; i < 5; i++) {
            clusterRunner.startDaemon();
        }

        final ImhotepClient client = clusterRunner.createClient();
        final ImhotepSession session = client.sessionBuilder(dataset, date, date.plusDays(duration)).build();
        final GroupStatsIterator result = session.getDistinct("if1", true);
        assertTrue(result.hasNext());
        assertEquals(0, result.nextLong());
        assertTrue(result.hasNext());
        assertEquals(110, result.nextLong());
        assertFalse(result.hasNext());
    }

    private FlamdexReader createReader(final int index) {
        final MemoryFlamdex flamdex = new MemoryFlamdex();
        final Function<Integer, FlamdexDocument> create = new Function<Integer, FlamdexDocument>() {
            @Override
            public FlamdexDocument apply(final Integer param) {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.setIntField("if1", param);
                return doc;
            }
        };
        // common part
        for (int i = 0; i < 10; i++) {
            flamdex.addDocument(create.apply(i));
        }
        // unique part
        for (int i = index * 10; i < (index + 1) * 10; i++) {
            flamdex.addDocument(create.apply(i));
        }
        return flamdex;
    }
}
