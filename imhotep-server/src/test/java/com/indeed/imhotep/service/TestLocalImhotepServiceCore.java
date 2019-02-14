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
import com.indeed.flamdex.simple.TestSimpleFlamdexDocWriter;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.imhotep.protobuf.ShardNameNumDocsPair;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * @author jsgroth
 */
public class TestLocalImhotepServiceCore {
    @BeforeClass
    public static void initLog4j() {
        TestSimpleFlamdexDocWriter.initLog4j();
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
                    new LocalImhotepServiceConfig(),
                    directory);


            final String sessionId = service.handleOpenSession("dataset", Collections.singletonList(ShardNameNumDocsPair.newBuilder().setShardName("index20150601").build()), "", "", "", 0, 0, false, "", null, 0);
            service.handlePushStat(sessionId, "count()");
            final OutputStream os = new CloseableNullOutputStream();
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        service.handleGetFTGSIterator(sessionId, new FTGSParams(new String[]{"if1"}, new String[0], 0, -1, true, null), os);
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
}
