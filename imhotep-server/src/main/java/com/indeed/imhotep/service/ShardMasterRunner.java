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
import com.google.common.collect.Lists;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.ShardMasterDaemon;
import com.indeed.imhotep.shardmasterrpc.RequestResponseClient;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * @author kornerup
 */
public class ShardMasterRunner {
    private final Path rootShardsDir;
    private final ServerSocket socket;
    private final List<Host> staticHostsList;

    @Nullable
    private ShardMaster dynamicShardMaster = null;
    @Nullable
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

    public void setDynamicShardMaster(@Nullable final ShardMaster dynamicShardMaster) {
        this.dynamicShardMaster = dynamicShardMaster;
    }

    public void start() throws IOException, TimeoutException, InterruptedException {
        if (currentlyRunning != null) {
            currentlyRunning.shutdown();
        }

        final String hostsString = StringUtils.join(staticHostsList, ",");

        final ShardMasterDaemon.Config config = new ShardMasterDaemon.Config()
                .setShardsRootPath(rootShardsDir.toString())
                .setServerSocket(socket)
                .setInitialRefreshReadFilesystem(true)
                .setReplicationFactor(1)
                .setLocalMode(true)
                .setReadSQL(false)
                .setWriteSQL(false)
                .setHostsListStatic(hostsString)
                .setDynamicShardMaster(dynamicShardMaster);
        currentlyRunning = new ShardMasterDaemon(config);

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
        return new RequestResponseClient(Lists.newArrayList(new Host("localhost", runner.getActualPort())));
    }
}
