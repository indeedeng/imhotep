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

import com.google.common.base.Throwables;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.ShardData;
import com.indeed.imhotep.shardmaster.ShardFilter;
import com.indeed.imhotep.shardmaster.ShardMasterDaemon;
import com.indeed.imhotep.shardmaster.ShardRefresher;
import com.indeed.imhotep.shardmasterrpc.RequestResponseClient;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author kornerup
 */
public class ShardMasterRunner {
    private final Path rootShardsDir;
    private final ServerSocket socket;
    private final List<Host> staticHostsList;

    private ShardMasterDaemon currentlyRunning;

    public ShardMasterRunner(final Path rootShardsDir,
                               final int port,
                               final List<Host> staticHostsList) throws IOException,
            TimeoutException {
        this.rootShardsDir = rootShardsDir;
        this.socket = new ServerSocket(port);
        this.staticHostsList = staticHostsList;
    }

    public int getActualPort() {
        return socket.getLocalPort();
    }

    public void start() throws IOException, TimeoutException, InterruptedException {
        if (currentlyRunning != null) {
            currentlyRunning.shutdown();
        }

        final String hostsString = StringUtils.join(staticHostsList, ",");

        currentlyRunning =
                new ShardMasterDaemon(new ShardMasterDaemon.Config()
                        .setShardsRootPath(rootShardsDir.toString())
                        .setServerSocket(socket)
                        .setLocalMode(true)
                        .setHostsListStatic(hostsString));

        new Thread(() -> {
            try {
                currentlyRunning.run();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }).start();
        currentlyRunning.waitForStartup(10000L);
    }

    public void stop() throws IOException {
        if (currentlyRunning != null) {
            currentlyRunning.shutdown();
            currentlyRunning = null;
        }
    }

    public static ShardMaster getFunctioningShardMaster(final Path rootShardsDir, final List<Host> staticHostsList) throws IOException, TimeoutException, InterruptedException {
        ShardMasterRunner runner = new ShardMasterRunner(rootShardsDir, 0, staticHostsList);
        runner.start();
        return new RequestResponseClient(new Host("localhost", runner.getActualPort()));
    }
}
