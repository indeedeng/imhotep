package com.indeed.imhotep.shardmaster;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.client.DummyHostsReloader;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.dbutil.DbDataFixture;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.fs.RemoteCachingFileSystemTestContext;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.shardmaster.db.shardinfo.Tables;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * @author kenh
 */

public class DatasetShardAssignmentRefresherTest {
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();
    @Rule
    public final DbDataFixture dbDataFixture = new DbDataFixture(Collections.singletonList(Tables.TBLSHARDASSIGNMENTINFO));
    @Rule
    public final RemoteCachingFileSystemTestContext fsTestContext = new RemoteCachingFileSystemTestContext();

    private final ExecutorService executorService = ShardMasterExecutors.newBlockingFixedThreadPool(10);

    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();

    private static void mapToProperties(final Map<String, String> config, final File target) throws IOException {
        final Properties properties = new Properties();
        for (final Map.Entry<String, String> entry : config.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }

        try (FileOutputStream os = new FileOutputStream(target)) {
            properties.store(os, "");
        }
    }

    private static File getConfigAsFile(final Map<String, String> config, final TemporaryFolder rootDir) throws IOException {
        final File configFile = rootDir.newFile("fs.properties");
        mapToProperties(config, configFile);
        return configFile;
    }

    private static List<Host> getHostsWithPrefix(final String prefix, final int port, final int start, final int end) {
        final List<Host> hosts = new ArrayList<>();
        for (int i = start; i < end; i++) {
            hosts.add(new Host(prefix + i, port));
        }
        return hosts;
    }

    private static void createShard(final File rootDir, final String dataset, final DateTime shardId, final long version) {
        Assert.assertTrue(new File(new File(rootDir, dataset), ShardTimeUtils.toDailyShardPrefix(shardId) + '.' + String.format("%014d", version)).mkdirs());
    }

    @After
    public void tearDown() {
        executorService.shutdown();
    }

    @Test
    public void testRefresh() throws ExecutionException, InterruptedException {
        final int numDataSets = 100;
        final int numShards = 15;
        for (int i = 0; i < numDataSets; i++) {
            final String dataset = "dataset" + i;
            for (int j = 0; j < numShards; j++) {
                final DateTime shard = TODAY.minusDays(j);
                createShard(fsTestContext.getLocalStoreDir(), dataset, shard, (Objects.hash(dataset, shard) % 10) + 11);
                // each shard has two versions, only 1 should be picked up
                createShard(fsTestContext.getLocalStoreDir(), dataset, shard, (Objects.hash(dataset, shard) % 10) + 13);
            }
        }

        final List<Host> hosts = getHostsWithPrefix("HOST", 8080, 0, 50);
        final int replicationFactor = 3;

        final RemoteCachingPath dataSetsDir = (RemoteCachingPath) Paths.get(RemoteCachingFileSystemProvider.URI);

        final ShardAssignmentInfoDao shardAssignmentInfoDao = new H2ShardAssignmentInfoDao(dbDataFixture.getDataSource(), Duration.standardMinutes(30));
        final DataSetScanWork.Result results = new DatasetShardAssignmentRefresher(
                dataSetsDir,
                ShardFilter.ACCEPT_ALL,
                executorService,
                new DummyHostsReloader(
                        hosts
                ),
                new MinHashShardAssigner(replicationFactor),
                shardAssignmentInfoDao
        ).initialize();

        final List<ShardScanWork.Result> datasetResult = results.getAllShards().get();
        Assert.assertEquals(numDataSets, datasetResult.size());
        for (final ShardScanWork.Result result : datasetResult) {
            Assert.assertEquals(numShards, result.getShards().size());
        }

        final Map<String, Integer> assignments = Maps.newHashMap();
        for (final Host host : hosts) {
            final List<ShardAssignmentInfo> infoList = Lists.newArrayList(shardAssignmentInfoDao.getAssignments(host));
            for (final ShardAssignmentInfo info : infoList) {
                final String id = info.getShardPath();
                final Integer count = assignments.get(id);
                if (count == null) {
                    assignments.put(id, 1);
                } else {
                    assignments.put(id, count + 1);
                }
            }
        }

        for (final Integer count : assignments.values()) {
            Assert.assertEquals(replicationFactor, count.intValue());
        }
    }

    @Test
    @Ignore("only for integration test purposes")
    public void testOnHdfs() throws IOException, ExecutionException, InterruptedException, SQLException, URISyntaxException {
        final Map<String, String> fsConfig = RemoteCachingFileSystemTestContext.getConfigFor(
                ImmutableMap.<String, String>builder()
                        .put("imhotep.fs.store.type", "hdfs")
                        .put("imhotep.fs.cache.size.gb", "1")
                        .build(),
                tempDir.newFolder("sqar"),
                tempDir.newFolder("cache"),
                tempDir.newFolder("local-store"),
                URI.create("hdfs:/var/imhotep"),
                new RemoteCachingFileSystemTestContext.TestS3Endpoint()
        );
        final File fsProp = getConfigAsFile(fsConfig, tempDir);

        RemoteCachingFileSystemProvider.newFileSystem(fsProp);

        final ShardAssignmentInfoDao shardAssignmentInfoDao = new H2ShardAssignmentInfoDao(dbDataFixture.getDataSource(), Duration.standardMinutes(30));
        final DataSetScanWork.Result results = new DatasetShardAssignmentRefresher(
                (RemoteCachingPath) Paths.get(RemoteCachingFileSystemProvider.URI),
                ShardFilter.ACCEPT_ALL,
                executorService,
                new DummyHostsReloader(
                        getHostsWithPrefix("HOST", 8080, 0, 50)
                ),
                new MinHashShardAssigner(3),
                shardAssignmentInfoDao
        ).initialize();

        for (final ShardScanWork.Result result : results.getAllShards().get()) {
            final RemoteCachingPath datasetDir = result.getDatasetDir();
            //noinspection UseOfSystemOutOrSystemErr
            System.out.println("Assigned " + result.getShards().size() + " for " + datasetDir);
        }
    }
}
