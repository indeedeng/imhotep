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

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.protobuf.ShardBasicInfoMessage;
import com.indeed.util.core.shell.PosixFileOperations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jsgroth
 */
public class TestLocalImhotepServiceCoreSharedResource {
    private static final long TIMEOUT = 5000L;

    private Path tempDir;
    private Path optDirectory;
    private static final String shardName = "index20160601";
    private Path datasetDir;

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory(this.getClass().getName());
        datasetDir = Files.createDirectory(tempDir.resolve("dataset"));
        Files.createDirectory(datasetDir.resolve(shardName));

        optDirectory = Files.createDirectory(tempDir.resolve("temp"));
    }

    @After
    public void tearDown() throws IOException {
        PosixFileOperations.rmrf(tempDir);
    }

    @Test
    public void testNoDoubleClose() throws IOException, ImhotepOutOfMemoryException {
        final FlamdexReaderSource factory = new FlamdexReaderSource() {
            @Override
            public FlamdexReader openReader(final Path directory) throws IOException {
                return new MockFlamdexReader(Collections.singletonList("if1"),
                                             Collections.singletonList("sf1"),
                                             Collections.singletonList("if1"), 10, directory) {
                    @Override
                    public long memoryRequired(final String metric) {
                        return Long.MAX_VALUE;
                    }
                };
            }

            @Override
            public FlamdexReader openReader(Path directory, int numDocs) throws IOException {
                return openReader(directory);
            }
        };

        final LocalImhotepServiceCore service =
                new LocalImhotepServiceCore(optDirectory, 1024L * 1024 * 1024,
                                        factory,
                                        new LocalImhotepServiceConfig(),
                        tempDir, new Host("localhost", 0));
        final String sessionId = service.handleOpenSession("dataset", Collections.singletonList(
                ShardBasicInfoMessage.newBuilder().setShardName(shardName).build())
                , "", "", "", 0, 0,
                false, "", null, 0, false);
        try {
            service.handlePushStat(sessionId, "if1");
            fail("pushStat didn't throw ImhotepOutOfMemory when it should have");
        } catch (final ImhotepOutOfMemoryException e) {
            // pass
        }
        service.handleCloseSession(sessionId);
        final String sessionId2 = service.handleOpenSession("dataset", Collections.singletonList(
                ShardBasicInfoMessage.newBuilder().setShardName(shardName).build()),
                "", "", "", 0, 0,
                false, "", null, 0, false);
        service.handleCloseSession(sessionId2);
        service.close();
    }

    @Test
    public void testActiveNoClose() throws IOException, ImhotepOutOfMemoryException, InterruptedException {
        final AtomicBoolean sessionClosed = new AtomicBoolean(false);
        final AtomicBoolean sessionOpened = new AtomicBoolean(false);
        final AtomicBoolean error = new AtomicBoolean(false);
        final AtomicBoolean done = new AtomicBoolean(false);

        final FlamdexReaderSource factory = new FlamdexReaderSource() {

            @Override
            public FlamdexReader openReader(final Path directory) throws IOException {
                return new MockFlamdexReader(Collections.singletonList("if1"),
                                             Collections.singletonList("sf1"),
                                             Collections.singletonList("if1"), 10, directory) {
                    @Override
                    public void close() throws IOException {
                        if (sessionOpened.get() && !sessionClosed.get()) {
                            error.set(true);
                        } else if (sessionOpened.get()) {
                            done.set(true);
                        }
                    }
                };
            }

            @Override
            public FlamdexReader openReader(Path directory, int numDocs) throws IOException {
                return openReader(directory);
            }
        };

        final LocalImhotepServiceCore service =
                new LocalImhotepServiceCore(
                                            optDirectory,
                                            Long.MAX_VALUE,
                                            factory,
                                            new LocalImhotepServiceConfig().setFlamdexReaderCacheMaxDurationMillis(1),
                                            tempDir,
                                            new Host("localhost", 0));
        try {
            final String sessionId = service.handleOpenSession("dataset", Collections.singletonList(
                    ShardBasicInfoMessage.newBuilder().setShardName(shardName).build()),
                    "", "", "", 0, 0,
                    false, "", null, 0, false);
            sessionOpened.set(true);
            try {
                for (int i = 0; i < 5; ++i) {
                    Thread.sleep(1000);
                    assertFalse("reader closed while still open in a session", error.get());
                }
                sessionClosed.set(true);
            } finally {
                service.handleCloseSession(sessionId);
            }
            Thread.sleep(2000);
            assertTrue("reader was never closed", done.get());
        } finally {
            service.close();
        }
    }
}
