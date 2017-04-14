package com.indeed.imhotep.service;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * @author kenh
 */

public class ImhotepDaemonClusterRunner {
    final List<ImhotepDaemonRunner> runners = new ArrayList<>();
    final File shardsDir;
    final File tempRootDir;
    final ImhotepShardCreator shardCreator;

    private static final DateTimeFormatter SHARD_VERSION_FORMAT = DateTimeFormat.forPattern(".yyyyMMddHHmmss");

    public ImhotepDaemonClusterRunner(final File shardsDir, final File tempRootDir, final ImhotepShardCreator shardCreator) {
        this.shardsDir = shardsDir;
        this.tempRootDir = tempRootDir;
        this.shardCreator = shardCreator;
    }

    public ImhotepDaemonClusterRunner(final File shardsDir, final File tempRootDir) {
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

    ImhotepDaemonRunner startDaemon() throws IOException, TimeoutException {
        return startDaemon(shardsDir.toPath());
    }

    ImhotepDaemonRunner startDaemon(final Path daemonShardsDir) throws IOException, TimeoutException {
        final ImhotepDaemonRunner runner = new ImhotepDaemonRunner(daemonShardsDir,
                // each daemon should have its own private scratch temp dir
                tempRootDir.toPath().resolve(UUID.randomUUID().toString()),
                0, new GenericFlamdexReaderSource());
        runner.start();
        runners.add(runner);

        return runner;
    }

    private List<Host> getDaemonHosts() {
        return FluentIterable.from(runners).transform(new Function<ImhotepDaemonRunner, Host>() {
            @Override
            public Host apply(final ImhotepDaemonRunner imhotepDaemonRunner) {
                return new Host("localhost", imhotepDaemonRunner.getActualPort());
            }
        }).toList();
    }

    public ImhotepClient createClient() {
        return new ImhotepClient(getDaemonHosts());
    }

    public void stop() throws IOException {
        for (final ImhotepDaemonRunner runner : runners) {
            runner.stop();
        }
    }
}
