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

package com.indeed.imhotep.shardmaster;

import com.indeed.imhotep.archive.SquallArchiveWriter;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.dbutil.DbDataFixture;
import com.indeed.imhotep.fs.RemoteCachingFileSystemTestContext;
import com.indeed.imhotep.shardmaster.db.shardinfo.Tables;
import com.indeed.imhotep.shardmasterrpc.ShardMasterExecutors;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

public class DatasetShardRefresherTest {
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();
    @Rule
    public final DbDataFixture dbDataFixture = new DbDataFixture(Collections.singletonList(Tables.TBLSHARDASSIGNMENTINFO));
    @Rule
    public final RemoteCachingFileSystemTestContext fsTestContext = new RemoteCachingFileSystemTestContext();

    private final ExecutorService executorService = ShardMasterExecutors.newBlockingFixedThreadPool(10);

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

    @Before
    public void createMetadataFile() throws IOException {
        java.nio.file.Path p = Paths.get(fsTestContext.getTempRootDir().toString(), "metadata");
        new File(p.toString()).mkdir();
        FlamdexMetadata.writeMetadata(p, new FlamdexMetadata(0, new ArrayList<>(), new ArrayList<>(), FlamdexFormatVersion.SIMPLE));

    }

    private void createShard(final File rootDir, final String dataset, final DateTime shardId, final long version) throws IOException{
        Path path = new Path(rootDir.toString() + "/" + dataset + "/" + ShardTimeUtils.toDailyShardPrefix(shardId) + "." + String.format("%014d", version) + ".sqar/");
        new File(path.toString()).mkdir();
        SquallArchiveWriter writer = new SquallArchiveWriter(path.getFileSystem(new Configuration()), new Path(path.toString()), true);
        File f = new File(fsTestContext.getTempRootDir()+"/metadata/metadata.txt");
        writer.appendFile(f);
        writer.commit();
    }

    @After
    public void tearDown() {
        executorService.shutdown();
    }

    @Test
    public void testRefresh() throws ExecutionException, InterruptedException, SQLException, IOException {
        final int numDataSets = 100;
        final int numShards = 15;
        final DateTime endDate = new DateTime(2018, 1, 1, 0, 0);

        for (int i = 0; i < numDataSets; i++) {
            final String dataset = "dataset" + i;
            for (int j = 0; j < numShards; j++) {
                final DateTime shard = endDate.minusDays(j);
                createShard(fsTestContext.getLocalStoreDir(), dataset, shard, (Objects.hash(dataset, shard) % 10) + 11);
                // each shard has two versions, only 1 should be picked up
                createShard(fsTestContext.getLocalStoreDir(), dataset, shard, (Objects.hash(dataset, shard) % 10) + 13);
            }
        }

        final List<Host> hosts = getHostsWithPrefix("HOST", 8080, 0, 50);
        final int replicationFactor = 3;

        final Path dataSetsDir = new Path("file:///" + fsTestContext.getLocalStoreDir());

        ZooKeeperConnection fakeZookeeperConnection = new ZooKeeperConnection("", 0);
        Connection fakeConnection = EasyMock.createNiceMock(Connection.class);
        PreparedStatement fakeStatement = EasyMock.createNiceMock(PreparedStatement.class);
        ResultSet fakeResults = EasyMock.createNiceMock(ResultSet.class);

        EasyMock.makeThreadSafe(fakeResults, true);
        EasyMock.makeThreadSafe(fakeStatement, true);
        EasyMock.makeThreadSafe(fakeConnection, true);

        EasyMock.expect(fakeStatement.executeQuery()).andReturn(fakeResults).anyTimes();

        EasyMock.expect(fakeConnection.prepareStatement(EasyMock.anyObject())).andReturn(fakeStatement).anyTimes();

        EasyMock.expect(fakeResults.first()).andReturn(false).anyTimes();

        EasyMock.replay(fakeConnection);
        EasyMock.replay(fakeStatement);
        EasyMock.replay(fakeResults);

        /*final ShardAssignmentInfoDao shardAssignmentInfoDao = new H2ShardAssignmentInfoDao(dbDataFixture.getDataSource(), Duration.standardMinutes(30));
        final Future results = new DatasetShardRefresher(
                dataSetsDir,
                new DummyHostsReloader(
                        hosts
                ),
                new MinHashShardAssigner(replicationFactor),
                shardAssignmentInfoDao,
                "",
                fakeZookeeperConnection,
                fakeConnection,
                fsTestContext.getLocalStoreDir().toString(),
                ShardFilter.ACCEPT_ALL).initialize();

        results.get();
        ShardData data = ShardData.getInstance();

        Assert.assertEquals(numDataSets, data.getDatasets().size());
        for (final String dataset : data.getDatasets()) {
            Assert.assertEquals(numShards, data.getShardsForDataset(dataset).size());
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
        }*/
    }

    // TODO: fix the integration test
    /*
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
        final Future results = new DatasetShardRefresher(
                (RemoteCachingPath) Paths.get(RemoteCachingFileSystemProvider.URI),
                new DummyHostsReloader(
                        getHostsWithPrefix("HOST", 8080, 0, 50)
                ),
                new MinHashShardAssigner(3),
                shardAssignmentInfoDao,
                null,
                null,
                null,
                fsTestContext.getLocalStoreDir().toString()).initialize();

        results.get();

        for (final ShardScanWork.Result result : results.getAllShards().get()) {
            final RemoteCachingPath datasetDir = result.getDatasetDir();
            //noinspection UseOfSystemOutOrSystemErr
            System.out.println("Assigned " + result.getShards().size() + " for " + datasetDir);
        }
    }*/
}
