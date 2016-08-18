package com.indeed.imhotep.service;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardTimeUtils;
import org.joda.time.DateTime;

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
    final File shardRootDir;
    final File tempRootDir;

    public ImhotepDaemonClusterRunner(final File shardRootDir, final File tempRootDir) {
        this.shardRootDir = shardRootDir;
        this.tempRootDir = tempRootDir;
    }

    public ImhotepDaemonClusterRunner(final File shardRootDir) {
        this.shardRootDir = shardRootDir;
        tempRootDir = new File(shardRootDir, "temp");
    }

    public void createDailyShard(final String dataset, final DateTime dateTime, final MemoryFlamdex memoryFlamdex) throws IOException {
        createShard(dataset, ShardTimeUtils.toDailyShardPrefix(dateTime), memoryFlamdex);
    }

    public void createHourlyShard(final String dataset, final DateTime dateTime, final MemoryFlamdex memoryFlamdex) throws IOException {
        createShard(dataset, ShardTimeUtils.toHourlyShardPrefix(dateTime), memoryFlamdex);
    }

    private void createShard(final String dataset, final String shardId, final MemoryFlamdex memoryFlamdex) throws IOException {
        final Path shardDir = shardRootDir.toPath().resolve(dataset).resolve(shardId);
        SimpleFlamdexWriter.writeFlamdex(memoryFlamdex, new SimpleFlamdexWriter(shardDir, memoryFlamdex.getNumDocs()));
    }

    ImhotepDaemonRunner startDaemon(final Path rootDir) throws IOException, TimeoutException {
        // each daemon should have its own private scratch temp dir
        final ImhotepDaemonRunner runner = new ImhotepDaemonRunner(rootDir, tempRootDir.toPath().resolve(UUID.randomUUID().toString()), 0, new GenericFlamdexReaderSource());
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
