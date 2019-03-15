package com.indeed.imhotep;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.reader.FlamdexFormatVersion;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.simple.TestSimpleFlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.imhotep.service.ImhotepDaemonRunner;
import com.indeed.imhotep.service.ImhotepShardCreator;
import com.indeed.imhotep.service.ShardLocator;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import com.indeed.imhotep.service.ShardMasterRunner;
import com.indeed.imhotep.shardmasterrpc.RequestResponseClient;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

// TODO: move to shardmaster tests
public class TestShardMaster {
    private static final DateTime TODAY = DateTime.now().withZone(DateTimeZone.forOffsetHours(-6)).withTimeAtStartOfDay();
    private Closer closeAfterTestCloser;

    @BeforeClass
    public static void initLog4j() {
        TestSimpleFlamdexDocWriter.initLog4j();
    }

    private ShardMasterAndImhotepDaemonClusterRunner clusterRunner;
    private Path storeDir;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        closeAfterTestCloser = Closer.create();
        storeDir = Files.createTempDirectory("temp-imhotep");
        tempDir = Files.createTempDirectory("temp-imhotep");
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                storeDir.toFile(),
                tempDir.toFile(),
                ImhotepShardCreator.DEFAULT
        );
    }

    @After
    public void tearDown() throws IOException {
        clusterRunner.stop();
        FileUtils.deleteDirectory(tempDir.toFile());
        FileUtils.deleteDirectory(storeDir.toFile());
        closeAfterTestCloser.close();
    }

    private ShardMaster getFunctioningShardMaster(final Path rootShardsDir, final List<Host> staticHostsList) throws IOException, TimeoutException, InterruptedException {
        final ShardMasterRunner runner = new ShardMasterRunner(rootShardsDir, 0, staticHostsList);
        runner.start();
        closeAfterTestCloser.register(runner::stop);
        return new RequestResponseClient(Lists.newArrayList(new Host("localhost", runner.getActualPort())));
    }

    @Test
    @SuppressWarnings({"ResultOfMethodCallIgnored"})
    public void testReadingShards() throws IOException, TimeoutException, InterruptedException {
        final Path directory = Files.createTempDirectory("imhotep-test");
        try {
            final Path datasetDir = directory.resolve("dataset");
            Files.createDirectory(datasetDir);
            createShardDirAndMetadataFile(datasetDir, "index20160101");
            createShardDirAndMetadataFile(datasetDir, "index20160101.20120101000000");
            createShardDirAndMetadataFile(datasetDir, "index20160101.20111231000000");
            createShardDirAndMetadataFile(datasetDir, "index20160102.20120101000000");
            createShardDirAndMetadataFile(datasetDir, "index20160102.20120101123456");
            createShardDirAndMetadataFile(datasetDir, "index20160103.20120102000000");

            final ShardMaster shardMaster = getFunctioningShardMaster(directory, Collections.singletonList(new Host("localhost", 0)));
            final Map<String, Collection<ShardInfo>> shardList = shardMaster.getShardList();
            final List<ShardInfo> shards = new ArrayList<>(shardList.get("dataset"));
            assertEquals(6, shards.size());
        } finally {
            TestFileUtils.deleteDirTree(directory);
        }
    }

    @Test
    public void testDynamicShardMaster() throws InterruptedException, TimeoutException, IOException {
        final MemoryFlamdex memoryFlamdex = closeAfterTestCloser.register(new MemoryFlamdex());
        memoryFlamdex.addDocument(new FlamdexDocument.Builder().addIntTerm("unixtime", TODAY.getMillis() / 1000).build());
        clusterRunner.createDailyShard("dataset", TODAY.minusDays(1), memoryFlamdex);
        clusterRunner.createDailyShard("dataset", TODAY, memoryFlamdex);

        final Path dynamicDir = Files.createDirectories(tempDir.resolve("dynamic"));
        final ShardDir dynamicShard = new ShardDir(dynamicDir.resolve(ShardTimeUtils.versionizeShardId(ShardTimeUtils.toDailyShardPrefix(TODAY.plusDays(1)))));
        createShardDirAndMetadataFile(dynamicDir, dynamicShard.getName(), 1);

        final AtomicReference<Host> daemonHost = new AtomicReference<>();
        final ShardMaster shardMaster = new ShardMaster() {
            @Override
            public List<DatasetInfo> getDatasetMetadata() {
                return Collections.emptyList();
            }

            @Override
            public List<Shard> getShardsInTime(final String dataset, final long start, final long end) {
                final Shard shard = new Shard(dynamicShard.getId(), 1, dynamicShard.getVersion(), daemonHost.get());
                if (shard.getStart().isAfter(end - 1) || shard.getEnd().isBefore(start + 1)) {
                    return Collections.emptyList();
                } else {
                    return Collections.singletonList(shard);
                }
            }

            @Override
            public Map<String, Collection<ShardInfo>> getShardList() {
                return Collections.emptyMap();
            }

            @Override
            public void refreshFieldsForDataset(final String dataset) {
            }
        };
        final ShardLocator shardLocator = (dataset, shardHostInfo) -> {
            final String shardName = shardHostInfo.getShardName();
            if (shardName.equals(dynamicShard.getName())) {
                return Optional.of(dynamicShard.getIndexDir());
            } else {
                return Optional.empty();
            }
        };
        clusterRunner.setDynamicShardMasterAndLocator(shardMaster, shardLocator);

        final ImhotepDaemonRunner imhotepDaemonRunner = clusterRunner.startDaemon();
        closeAfterTestCloser.register(imhotepDaemonRunner::stop);
        daemonHost.set(new Host("localhost", imhotepDaemonRunner.getActualPort()));

        try (final ImhotepClient client = clusterRunner.createClient()) {
            Assert.assertEquals(2, client.findShardsForTimeRange("dataset", TODAY.minusDays(1), TODAY.plusDays(1)).size());
            try (final ImhotepSession session = client.sessionBuilder("dataset", TODAY.minusDays(1), TODAY.plusDays(1)).build()) {
                Assert.assertEquals(2, session.getNumDocs());
            }
            Assert.assertEquals(3, client.findShardsForTimeRange("dataset", TODAY.minusDays(1), TODAY.plusDays(2)).size());
            try (final ImhotepSession session = client.sessionBuilder("dataset", TODAY.minusDays(1), TODAY.plusDays(2)).build()) {
                Assert.assertEquals(3, session.getNumDocs());
            }
        }
    }

    private void createShardDirAndMetadataFile(final Path datasetDir, final String shardName) throws IOException {
        createShardDirAndMetadataFile(datasetDir, shardName, 0);
    }

    private void createShardDirAndMetadataFile(final Path datasetDir, final String shardName, final int numDocs) throws IOException {
        final Path directory = Files.createDirectory(datasetDir.resolve(shardName));
        FlamdexMetadata.writeMetadata(directory, new FlamdexMetadata(numDocs, new ArrayList<>(), new ArrayList<>(), FlamdexFormatVersion.SIMPLE));
    }
}
