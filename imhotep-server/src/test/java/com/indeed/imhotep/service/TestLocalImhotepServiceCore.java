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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.imhotep.protobuf.ShardNameNumDocsPair;
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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

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

        final LevelRangeFilter ERROR_FILTER = new LevelRangeFilter();
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
        final Path directory = Files.createTempDirectory("asdf");
        final Path tempDir = Files.createTempDirectory("asdf");
        final Path datasetDir = directory.resolve("dataset");
        Files.createDirectories(datasetDir);
        Files.createDirectories(datasetDir.resolve("index20150601"));
        try {
            final LocalImhotepServiceCore service = new LocalImhotepServiceCore(tempDir, 9999999999999L, new FlamdexReaderSource() {
                @Override
                public FlamdexReader openReader(final Path directory) throws IOException {
                    final MockFlamdexReader r =
                            new MockFlamdexReader(
                                    Collections.singletonList("if1"),
                                    Collections.<String>emptyList(),
                                    Collections.<String>emptyList(),
                                    10000, directory);
                    for (int i = 0; i < 1000; ++i) {
                        for (int j = 0; j < 1000; ++j) {
                            r.addIntTerm("if1", i * 1000 + j, Collections.singletonList(0));
                        }
                    }
                    return r;
                }

                @Override
                public FlamdexReader openReader(Path directory, int numDocs) throws IOException {
                    return openReader(directory);
                }
            },
                    new LocalImhotepServiceConfig());

            final String sessionId = service.handleOpenSession("dataset", Collections.singletonList(ShardNameNumDocsPair.newBuilder().setShardName("index20150601").build()), "", "", "", 0, 0, false, "", null, 0);
            service.handlePushStat(sessionId, "count()");
            final OutputStream os = new CloseableNullOutputStream();
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        service.handleGetFTGSIterator(sessionId, new FTGSParams(new String[]{"if1"}, new String[0], 0, -1, true), os);
                        fail();
                    } catch (final Exception expected) {
                        expected.printStackTrace();
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
            TestFileUtils.deleteDirTree(directory);
            TestFileUtils.deleteDirTree(tempDir);
        }
    }

    private static class CloseableNullOutputStream extends OutputStream {
        private volatile boolean closed = false;

        @Override
        public void write(final int b) throws IOException {
            if (closed) {
                throw new IOException("closed");
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }

    // TODO: move to shardmaster tests
    @Test
    @SuppressWarnings({"ResultOfMethodCallIgnored"})
    public void testVersionization() throws IOException {
        final Path directory = Files.createTempDirectory("imhotep-test");
        final Path tempDir = Files.createTempDirectory("imhotep-temp");
        try {
            final Path datasetDir = directory.resolve("dataset");
            Files.createDirectory(datasetDir);
            Files.createDirectory(datasetDir.resolve("index20160101"));
            Files.createDirectory(datasetDir.resolve("index20160101.20120101000000"));
            Files.createDirectory(datasetDir.resolve("index20160101.20111231000000"));
            Files.createDirectory(datasetDir.resolve("index20160102.20120101000000"));
            Files.createDirectory(datasetDir.resolve("index20160102.20120101123456"));
            Files.createDirectory(datasetDir.resolve("index20160103.20120102000000"));

            final LocalImhotepServiceCore service =
                new LocalImhotepServiceCore(tempDir, Long.MAX_VALUE, new FlamdexReaderSource() {
                    @Override
                    public FlamdexReader openReader(final Path directory) throws IOException {
                        return new MockFlamdexReader(Collections.singletonList("if1"),
                                Collections.singletonList("sf1"),
                                Collections.singletonList("if1"), 5);
                    }

                    @Override
                    public FlamdexReader openReader(Path directory, int numDocs) throws IOException {
                        return openReader(directory);
                    }
                },
                        new LocalImhotepServiceConfig());
            //TODO: fix or abandon test
            final List<ShardInfo> shards = null; //Lists.newArrayList(service.handleGetDatasetList().get(0).getShardList());
            assertEquals(3, shards.size());
            Collections.sort(shards, new Comparator<ShardInfo>() {
                @Override
                public int compare(final ShardInfo o1, final ShardInfo o2) {
                    return o1.getShardId().compareTo(o2.getShardId());
                }
            });
            assertEquals("index20160101", shards.get(0).getShardId());
            assertEquals(20120101000000L, shards.get(0).getVersion());
            assertEquals("index20160102", shards.get(1).getShardId());
            assertEquals(20120101123456L, shards.get(1).getVersion());
            assertEquals("index20160103", shards.get(2).getShardId());
            assertEquals(20120102000000L, shards.get(2).getVersion());

            service.close();
        } finally {
            TestFileUtils.deleteDirTree(directory);
            TestFileUtils.deleteDirTree(tempDir);
        }
    }
}
