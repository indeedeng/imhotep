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
 package com.indeed.imhotep.service;

import com.indeed.util.io.Files;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.LevelRangeFilter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * @author jsgroth
 */
public class TestLocalImhotepServiceCore {
    @BeforeClass
    public static void initLog4j() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();

        final Layout LAYOUT = new PatternLayout("[ %d{ISO8601} %-5p ] [%c{1}] %m%n");

        LevelRangeFilter ERROR_FILTER = new LevelRangeFilter();
        ERROR_FILTER.setLevelMin(Level.ERROR);
        ERROR_FILTER.setLevelMax(Level.FATAL);

        // everything including ERROR
        final Appender STDOUT = new ConsoleAppender(LAYOUT, ConsoleAppender.SYSTEM_OUT);

        // just things <= ERROR
        final Appender STDERR = new ConsoleAppender(LAYOUT, ConsoleAppender.SYSTEM_ERR);
        STDERR.addFilter(ERROR_FILTER);

        final Logger ROOT_LOGGER = Logger.getRootLogger();

        ROOT_LOGGER.removeAllAppenders();

        ROOT_LOGGER.setLevel(Level.WARN); // don't care about higher

        ROOT_LOGGER.addAppender(STDOUT);
        ROOT_LOGGER.addAppender(STDERR);
    }

    @Test
    public void testCleanupOnFTGSFailure() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
        String directory = Files.getTempDirectory("asdf", "");
        String tempDir = Files.getTempDirectory("asdf", "");
        File datasetDir = new File(directory, "dataset");
        datasetDir.mkdir();
        new File(datasetDir, "shard").mkdir();
        try {
            final LocalImhotepServiceCore service = new LocalImhotepServiceCore(directory, tempDir, 9999999999999L, false, new FlamdexReaderSource() {
                @Override
                public FlamdexReader openReader(String directory) throws IOException {
                    MockFlamdexReader r = new MockFlamdexReader(Arrays.asList("if1"), Collections.<String>emptyList(), Collections.<String>emptyList(), 10000);
                    for (int i = 0; i < 1000; ++i) {
                        for (int j = 0; j < 1000; ++j) {
                            r.addIntTerm("if1", i * 1000 + j, Arrays.asList(0));
                        }
                    }
                    return r;
                }
            }, new LocalImhotepServiceConfig());

            final String sessionId = service.handleOpenSession("dataset", Arrays.asList("shard"), "", "", 0, 0, false, "", null, false);
            service.handlePushStat(sessionId, "count()");
            final OutputStream os = new CloseableNullOutputStream();
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        service.handleGetFTGSIterator(sessionId, new String[]{"if1"}, new String[0], os);
                        fail();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            t.start();
            os.close();

            t.join(5000);

            assertFalse(t.isAlive());

            service.handleCloseSession(sessionId);
            service.close();
        } finally {
            Files.delete(directory);
        }
    }

    private static class CloseableNullOutputStream extends OutputStream {
        private volatile boolean closed = false;

        @Override
        public void write(int b) throws IOException {
            if (closed) throw new IOException("closed");
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }

    @Test
    @SuppressWarnings({"ResultOfMethodCallIgnored"})
    public void testVersionization() throws IOException {
        String directory = Files.getTempDirectory("imhotep", "test");
        String tempDir = Files.getTempDirectory("imhotep", "temp");
        try {
            File datasetDir = new File(directory, "dataset");
            datasetDir.mkdir();
            new File(datasetDir, "shard0").mkdir();
            new File(datasetDir, "shard0.20120101000000").mkdir();
            new File(datasetDir, "shard0.20111231000000").mkdir();
            new File(datasetDir, "shard1.20120101000000").mkdir();
            new File(datasetDir, "shard1.20120101123456").mkdir();
            new File(datasetDir, "shard2.20120102000000").mkdir();

            LocalImhotepServiceCore service = new LocalImhotepServiceCore(directory, tempDir, Long.MAX_VALUE, false, new FlamdexReaderSource() {
                @Override
                public FlamdexReader openReader(String directory) throws IOException {
                    return new MockFlamdexReader(Arrays.asList("if1"), Arrays.asList("sf1"), Arrays.asList("if1"), 5);
                }
            }, new LocalImhotepServiceConfig());
            List<ShardInfo> shards = service.handleGetShardList();
            assertEquals(3, shards.size());
            Collections.sort(shards, new Comparator<ShardInfo>() {
                @Override
                public int compare(ShardInfo o1, ShardInfo o2) {
                    return o1.getShardId().compareTo(o2.getShardId());
                }
            });
            assertEquals("shard0", shards.get(0).getShardId());
            assertEquals(20120101000000L, shards.get(0).getVersion());
            assertEquals("shard1", shards.get(1).getShardId());
            assertEquals(20120101123456L, shards.get(1).getVersion());
            assertEquals("shard2", shards.get(2).getShardId());
            assertEquals(20120102000000L, shards.get(2).getVersion());

            service.close();
        } finally {
            Files.delete(directory);
        }
    }
}
