package com.indeed.imhotep.service;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardTimeUtils;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author kenh
 */

public class ImhotepDaemonClusterRunner {
    final List<ImhotepDaemonRunner> runners = new ArrayList<>();
    final File rootDir;
    final File tempDir;
    private final Map<String, MemoryFlamdex> flamdexMap = new HashMap<>();
    private final FlamdexReaderSource factory = new FlamdexReaderSource() {

        @Override
        public FlamdexReader openReader(final String directory) throws IOException {
            return flamdexMap.get(directory);
        }
    };

    public ImhotepDaemonClusterRunner(final File rootDir) {
        this.rootDir = rootDir;
        tempDir = new File(rootDir, "temp");
    }

    public void createDailyShard(final String dataset, final DateTime dateTime, final MemoryFlamdex memoryFlamdex) throws IOException {
        createShard(dataset, ShardTimeUtils.toDailyShardPrefix(dateTime), memoryFlamdex);
    }

    public void createHourlyShard(final String dataset, final DateTime dateTime, final MemoryFlamdex memoryFlamdex) throws IOException {
        createShard(dataset, ShardTimeUtils.toHourlyShardPrefix(dateTime), memoryFlamdex);
    }

    private void createShard(final String dataset, final String shardId, final MemoryFlamdex memoryFlamdex) throws IOException {
        final File datasetDir = new File(rootDir, dataset);
        if (!datasetDir.exists() && !datasetDir.mkdir()) {
            throw new IOException("Failed to create directory " + datasetDir.getAbsolutePath());
        }

        final File shardDir = new File(datasetDir, shardId);
        if (!shardDir.mkdir()) {
            throw new IOException("couldn't make " + shardDir.getAbsolutePath());
        }

        flamdexMap.put(shardDir.getAbsolutePath(), memoryFlamdex);
    }

    ImhotepDaemonRunner startDaemon() throws IOException, TimeoutException {
        final ImhotepDaemonRunner runner = new ImhotepDaemonRunner(rootDir.getAbsolutePath(), tempDir.getAbsolutePath(), 0, factory);
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
