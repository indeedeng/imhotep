package com.indeed.imhotep;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.indeed.flamdex.reader.FlamdexFormatVersion;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.simple.TestSimpleFlamdexDocWriter;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.imhotep.service.ShardMasterRunner;
import com.indeed.imhotep.shardmasterrpc.RequestResponseClient;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import org.junit.After;
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
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

// TODO: move to shardmaster tests
public class TestShardMaster {
    private Closer closeAfterTestCloser;

    @BeforeClass
    public static void initLog4j() {
        TestSimpleFlamdexDocWriter.initLog4j();
    }

    @Before
    public void setUp() {
        closeAfterTestCloser = Closer.create();
    }

    @After
    public void tearDown() throws IOException {
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
        final Path tempDir = Files.createTempDirectory("imhotep-temp");
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
            TestFileUtils.deleteDirTree(tempDir);
        }
    }

    private void createShardDirAndMetadataFile(Path datasetDir, String shardName) throws IOException {
        final Path directory = Files.createDirectory(datasetDir.resolve(shardName));
        FlamdexMetadata.writeMetadata(directory, new FlamdexMetadata(0, new ArrayList<>(), new ArrayList<>(), FlamdexFormatVersion.SIMPLE));
    }
}
