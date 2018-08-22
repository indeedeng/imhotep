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
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardTimeUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author kenh
 */

public class ShardMasterAndImhotepDaemonClusterRunner {
    final List<ImhotepDaemonRunner> daemonRunners = new ArrayList<>();
    final File shardsDir;
    final File tempRootDir;
    final ImhotepShardCreator shardCreator;

    private static final DateTimeFormatter SHARD_VERSION_FORMAT = DateTimeFormat.forPattern(".yyyyMMddHHmmss");

    public ShardMasterAndImhotepDaemonClusterRunner(final File shardsDir, final File tempRootDir, final ImhotepShardCreator shardCreator) {
        this.shardsDir = shardsDir;
        this.tempRootDir = tempRootDir;
        this.shardCreator = shardCreator;
    }

    public ShardMasterAndImhotepDaemonClusterRunner(final File shardsDir, final File tempRootDir) {
        this(shardsDir, tempRootDir, ImhotepShardCreator.DEFAULT);
    }

    public void createDailyShard(final String dataset, final DateTime dateTime, final FlamdexReader flamdexReader) throws IOException {
        createShard(dataset, ShardTimeUtils.toDailyShardPrefix(dateTime) + SHARD_VERSION_FORMAT.print(DateTime.now()), flamdexReader);
    }

    public void createHourlyShard(final String dataset, final DateTime dateTime, final FlamdexReader flamdexReader) throws IOException {
        createShard(dataset, ShardTimeUtils.toHourlyShardPrefix(dateTime) + SHARD_VERSION_FORMAT.print(DateTime.now()), flamdexReader);
    }

    private void createShard(final String dataset, final String shardId, final FlamdexReader memoryFlamdex) throws IOException {
        shardCreator.create(shardsDir, dataset, shardId, memoryFlamdex);
    }

    public ImhotepDaemonRunner startDaemon() throws IOException, TimeoutException, InterruptedException {
        return startDaemon(shardsDir.toPath());
    }

    public ImhotepDaemonRunner startDaemon(final Path daemonShardsDir) throws IOException, TimeoutException, InterruptedException {
        final ImhotepDaemonRunner runner = new ImhotepDaemonRunner(daemonShardsDir,
                // each daemonRunners should have its own private scratch temp dir
                tempRootDir.toPath().resolve(UUID.randomUUID().toString()),
                0, new GenericFlamdexReaderSource());
        runner.start();
        daemonRunners.add(runner);
        return runner;
    }

    private List<Host> getDaemonHosts() {
        return daemonRunners.stream().map(runner -> new Host("localhost", runner.getActualPort())).collect(Collectors.toList());
    }

    public ImhotepClient createClient() throws InterruptedException, TimeoutException, IOException {
        ShardMasterRunner smRunner = new ShardMasterRunner(shardsDir.toPath(), 0, getDaemonHosts());
        smRunner.start();
        return new ImhotepClient(Collections.singletonList(new Host("localhost", smRunner.getActualPort())));
    }

    public void stop() throws IOException {
        for (final ImhotepDaemonRunner runner : daemonRunners) {
            runner.stop();
        }
    }
}
