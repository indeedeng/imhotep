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

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.reader.MockFlamdexReader;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

/**
 * @author jsgroth
 */
public class ImhotepDaemonRunner {
    private final Path dir;
    private final Path tempDir;
    private final int port;
    private int actualPort;
    private final FlamdexReaderSource flamdexFactory;

    private ImhotepDaemon currentlyRunning;

    public ImhotepDaemonRunner(final Path dir, final Path tempDir, final int port) throws IOException,
                                                                                      TimeoutException {
        this(dir, tempDir, port, new FlamdexReaderSource() {
            @Override
            public FlamdexReader openReader(Path directory) throws IOException {
                return new MockFlamdexReader();
            }
        });
    }

    public ImhotepDaemonRunner(final Path dir,
                               Path tempDir,
                               final int port,
                               final FlamdexReaderSource flamdexFactory) throws IOException,
                                                                        TimeoutException {
        this.dir = dir;
        this.tempDir = tempDir;
        this.port = port;
        this.flamdexFactory = flamdexFactory;        
    }

    public int getPort() {
        return port;
    }

    public int getActualPort() {
        return actualPort;
    }

    public void start() throws IOException, TimeoutException {
        if (currentlyRunning != null) {
            currentlyRunning.shutdown(false);
        }
        currentlyRunning =
                new ImhotepDaemon(new ServerSocket(port),
                                  new LocalImhotepServiceCore(dir, tempDir,
                                                              1024L * 1024 * 1024 * 1024, false,
                                                              flamdexFactory,
                                                              new LocalImhotepServiceConfig()),
                                  null, null, "localhost", port);
        actualPort = currentlyRunning.getPort();

        new Thread(new Runnable() {
            @Override
            public void run() {
                currentlyRunning.run();
            }
        }).start();
        currentlyRunning.waitForStartup(10000L);
    }

    public void stop() throws IOException {
        if (currentlyRunning != null) {
            currentlyRunning.shutdown(false);
            currentlyRunning = null;
        }
    }
}
