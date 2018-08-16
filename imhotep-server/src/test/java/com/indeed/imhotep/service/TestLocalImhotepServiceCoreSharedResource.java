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
import com.indeed.imhotep.protobuf.ShardNameNumDocsPair;
import com.indeed.util.core.shell.PosixFileOperations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
                        datasetDir);
        final String sessionId = service.handleOpenSession("dataset", Collections.singletonList(ShardNameNumDocsPair.newBuilder().setShardName(shardName).build()), "", "", "", 0, 0, false, "", null, 0);
        try {
            service.handlePushStat(sessionId, "if1");
            fail("pushStat didn't throw ImhotepOutOfMemory when it should have");
        } catch (final ImhotepOutOfMemoryException e) {
            // pass
        }
        service.handleCloseSession(sessionId);
        final String sessionId2 = service.handleOpenSession("dataset", Collections.singletonList(ShardNameNumDocsPair.newBuilder().setShardName(shardName).build()), "", "", "", 0, 0, false, "", null, 0);
        service.handleCloseSession(sessionId2);
        service.close();
    }

    @Test
    public void testReloadCloses() throws IOException, InterruptedException {
        final AtomicBoolean closed = new AtomicBoolean(false);
        final AtomicBoolean created = new AtomicBoolean(false);
        final FlamdexReaderSource factory = new FlamdexReaderSource() {
            int i = 0;

            @Override
            public FlamdexReader openReader(final Path directory) throws IOException {
                while (!created.compareAndSet(false, true)) {}

                if (((i++) & 1) == 0) {
                    return new MockFlamdexReader(Collections.singletonList("if1"),
                                                 Collections.<String>emptyList(),
                                                 Collections.singletonList("if1"), 10) {
                        @Override
                        public void close() throws IOException {
                            while (!closed.compareAndSet(false, true)) {}
                        }
                    };
                } else {
                    return new MockFlamdexReader(Collections.<String>emptyList(), Collections.singletonList("sf1"), Collections.<String>emptyList(), 10) {
                        @Override
                        public void close() throws IOException {
                            while (!closed.compareAndSet(false, true)) {}
                        }
                    };
                }
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
                                            new LocalImhotepServiceConfig().setUpdateShardsFrequencySeconds(1),
                                            datasetDir);

        try {
            final long initial = System.currentTimeMillis();
            boolean b;
            while (!(b = created.compareAndSet(true, false)) && (System.currentTimeMillis() - initial) < TIMEOUT) {
            }
            assertTrue("first index took too long to be created", b);
            final long t = System.currentTimeMillis();
            while (!(b = closed.compareAndSet(true, false)) && (System.currentTimeMillis() - t) < TIMEOUT) {
            }
            assertTrue("close took too long", b);
        } finally {
            service.close();
        }
    }

    @Test
    public void testNoReloadNoClose() throws IOException {
        final AtomicInteger createCount = new AtomicInteger(0);
        final AtomicBoolean error = new AtomicBoolean(false);
        final FlamdexReaderSource factory = new FlamdexReaderSource() {
            FlamdexReader lastOpened = null;

            @Override
            public FlamdexReader openReader(final Path directory) throws IOException {
                createCount.incrementAndGet();

                lastOpened = new MockFlamdexReader(Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList(), 10) {
                    @Override
                    public void close() throws IOException {
                        if (lastOpened != this) {
                            error.set(true);
                        }
                    }
                };
                return lastOpened;
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
                                            new LocalImhotepServiceConfig().setUpdateShardsFrequencySeconds(1),
                                            datasetDir);
        try {
            final long t = System.currentTimeMillis();
            boolean b = true;
            boolean problem;
            while (!(problem = error.get()) && (b = createCount.get() < 1) && (System.currentTimeMillis() - t) < TIMEOUT) {
            }
            assertFalse("creates took too long", b);
            assertFalse("close called on a reader that it shouldn't have been", problem);
        } finally {
            service.close();
        }
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
                                            new LocalImhotepServiceConfig().setUpdateShardsFrequencySeconds(1),
                                            datasetDir);
        try {
            final String sessionId = service.handleOpenSession("dataset", Collections.singletonList(ShardNameNumDocsPair.newBuilder().setShardName(shardName).build()), "", "", "", 0, 0, false, "", null, 0);
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
