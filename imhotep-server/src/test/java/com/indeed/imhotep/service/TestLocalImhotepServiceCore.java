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
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.MockShard;
import com.indeed.imhotep.io.Shard;
import com.indeed.imhotep.io.TestFileUtils;
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
            final LocalImhotepServiceCore service = new LocalImhotepServiceCore(directory, tempDir, 9999999999999L, new FlamdexReaderSource() {
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
            }, new LocalImhotepServiceConfig());

            final String sessionId = service.handleOpenSession("dataset", Collections.singletonList("index20150601"), "", "", "", 0, 0, false, "", null, false, 0);
            service.handlePushStat(sessionId, "count()");
            final OutputStream os = new CloseableNullOutputStream();
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        service.handleGetFTGSIterator(sessionId, new String[]{"if1"}, new String[0], 0, -1, os);
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
                new LocalImhotepServiceCore(directory, tempDir, Long.MAX_VALUE, new FlamdexReaderSource() {
                @Override
                public FlamdexReader openReader(final Path directory) throws IOException {
                    return new MockFlamdexReader(Collections.singletonList("if1"),
                                                 Collections.singletonList("sf1"),
                                                 Collections.singletonList("if1"), 5);
                }
            }, new LocalImhotepServiceConfig());
            final List<ShardInfo> shards = Lists.newArrayList(service.handleGetDatasetList().get(0).getShardList());
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

    @Test
    public void testBuildDatasetList() throws IOException {
        final String dataset = "sponsored";
        Set<String> expectedIntFields;
        Set<String> expectedStringFields;

        ShardMap localShards = new ShardMap((MemoryReserver)null, null);
        addShard(localShards, dataset, 20150101000000L,
                 ImmutableSet.of("int1"), ImmutableSet.of("str1"));
        addShard(localShards, dataset, 20150102000000L,
                 ImmutableSet.of("int1", "int2"), ImmutableSet.of("str1", "str2"));
        addShard(localShards, dataset, 20150103000000L,
                 ImmutableSet.of("int1", "int2"), ImmutableSet.of("str1", "str2"));
        expectedIntFields = ImmutableSet.of("int1", "int2");
        expectedStringFields = ImmutableSet.of("str1", "str2");
        checkExpectedFields(localShards, expectedIntFields, expectedStringFields);

        localShards = new ShardMap((MemoryReserver)null, null);
        addShard(localShards, dataset, 20150101000000L,
                 ImmutableSet.of("int1", "int2", "conflict"),
                 ImmutableSet.of("str1", "str2", "conflict"));
        addShard(localShards, dataset, 20150102000000L,
                 ImmutableSet.of("int1", "int2", "conflict"),
                 ImmutableSet.of("str1", "str2", "conflict"));
        addShard(localShards, dataset, 20150103000000L,
                 ImmutableSet.of("int1", "int2", "conflict"),
                 ImmutableSet.of("str1", "str2"));
        expectedIntFields = ImmutableSet.of("int1", "int2", "conflict");
        expectedStringFields = ImmutableSet.of("str1", "str2");
        checkExpectedFields(localShards, expectedIntFields, expectedStringFields);

        localShards = new ShardMap((MemoryReserver)null, null);
        addShard(localShards, dataset, 20150103000000L,
                 ImmutableSet.of("conflict", "int1", "int2"),
                 ImmutableSet.of("str1", "str2"));
        addShard(localShards, dataset, 20150101000000L,
                 ImmutableSet.of("conflict", "int1", "int2"),
                 ImmutableSet.of("str1", "str2", "conflict"));
        addShard(localShards, dataset, 20150102000000L,
                 ImmutableSet.of("conflict", "int1", "int2"),
                 ImmutableSet.of("str1", "str2", "conflict"));
        expectedIntFields = ImmutableSet.of("int1", "int2", "conflict");
        expectedStringFields = ImmutableSet.of("str1", "str2");
        checkExpectedFields(localShards, expectedIntFields, expectedStringFields);

        localShards = new ShardMap((MemoryReserver)null, null);
        addShard(localShards, dataset, 20150103000000L,
                 ImmutableSet.of("conflict", "int1", "int2"),
                 ImmutableSet.of("str1", "str2", "conflict"));
        addShard(localShards, dataset, 20150101000000L,
                 ImmutableSet.of("conflict", "int1", "int2"),
                 ImmutableSet.of("str1", "str2", "conflict"));
        addShard(localShards, dataset, 20150102000000L,
                 ImmutableSet.of("conflict", "int1", "int2"),
                 ImmutableSet.of("str1", "str2", "conflict"));
        expectedIntFields = ImmutableSet.of("int1", "int2", "conflict");
        expectedStringFields = ImmutableSet.of("str1", "str2", "conflict");
        checkExpectedFields(localShards, expectedIntFields, expectedStringFields);
    }

    private void checkExpectedFields(final ShardMap localShards,
                                     final Set<String> expectedIntFields,
                                     final Set<String> expectedStringFields) throws IOException {
        final DatasetInfoList datasetInfos = new DatasetInfoList(localShards);
        assertEquals(1, datasetInfos.size());

        final DatasetInfo datasetInfo = datasetInfos.get(0);
        assertEquals(expectedIntFields, datasetInfo.getIntFields());
        assertEquals(expectedStringFields, datasetInfo.getStringFields());
    }

    private void addShard(final ShardMap localShards,
                          final String dataset,
                          final long version,
                          final ImmutableSet<String> intFields,
                          final ImmutableSet<String> stringFields) throws IOException {
        final String shardId = "index" + Long.toString(version).substring(0, 8);
        final Shard shard = new MockShard(new ShardId(dataset, shardId, version, null),
                                    0, intFields, stringFields);
        localShards.putShard(dataset, shard);
    }
}
