package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.indeed.imhotep.ZkEndpointPersister;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.fs.RemoteCachingFileSystemTestContext;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;
import com.indeed.imhotep.shardmaster.rpc.RequestResponseClient;
import com.indeed.imhotep.shardmaster.rpc.RequestResponseClientFactory;
import com.indeed.util.core.Pair;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

/**
 * @author kenh
 */

public class ShardMasterDaemonTest {
    @Rule
    public final RemoteCachingFileSystemTestContext testContext = new RemoteCachingFileSystemTestContext();
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();
    private final Closer closer = Closer.create();
    private TestingServer testingServer;

    private static final DateTime WEEK_AGO = DateTime.now().minusDays(7).withTimeAtStartOfDay();

    @Before
    public void setUp() throws Exception {
        testingServer = new TestingServer();
    }

    @After
    public void tearDown() throws IOException {
        closer.close();
        testingServer.close();
    }

    private void createShard(final String dataset, final DateTime shardId, final long version) {
        Assert.assertTrue(new File(new File(testContext.getLocalStoreDir(), dataset), ShardTimeUtils.toDailyShardPrefix(shardId) + '.' + String.format("%014d", version)).mkdirs());
    }

    private void createShard(final String dataset, final String shardId, final long version) {
        Assert.assertTrue(new File(new File(testContext.getLocalStoreDir(), dataset), shardId + '.' + String.format("%014d", version)).mkdirs());
    }

    @Test
    public void testIt() throws IOException, InterruptedException, ExecutionException, KeeperException {
        final int replicationFactor = 2;
        final Host daemon1 = new Host("HOST1", 1230);
        final Host daemon2 = new Host("HOST1", 2340);
        final Host daemon3 = new Host("HOST2", 1230);

        final ShardMasterDaemon.Config config = new ShardMasterDaemon.Config()
                .setReplicationFactor(replicationFactor)
                .setZkNodes(testingServer.getConnectString())
                .setImhotepDaemonsZkPath("/imhotep/daemons")
                .setShardMastersZkPath("/imhotep/shardmasters")
                .setDbFile(new File(tempDir.getRoot(), "db.dat").toString())
                .setHostsFile(new File(tempDir.getRoot(), "hosts.dat").toString());

        closer.register(new ZkEndpointPersister(testingServer.getConnectString(), "/imhotep/daemons", daemon1));
        closer.register(new ZkEndpointPersister(testingServer.getConnectString(), "/imhotep/daemons", daemon2));
        closer.register(new ZkEndpointPersister(testingServer.getConnectString(), "/imhotep/daemons", daemon3));

        createShard("dataset1", WEEK_AGO, 4);
        createShard("dataset1", WEEK_AGO, 1);
        createShard("dataset1", WEEK_AGO.plusDays(1), 10);
        createShard("dataset1", WEEK_AGO.plusDays(2), 11);
        createShard("dataset1", "garbage", 1);

        createShard("dataset2", WEEK_AGO, 12);

        createShard("dataset3", WEEK_AGO, 1);
        createShard("dataset3", WEEK_AGO, 5);
        createShard("dataset3", WEEK_AGO.plusDays(1), 10);

        final ShardMasterDaemon shardMasterDaemon = new ShardMasterDaemon(config);
        final Thread daemonRunner = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    shardMasterDaemon.run();
                } catch (final Throwable e) {
                    Assert.fail("Unexpected error while running daemon " + e);
                }
            }
        });
        daemonRunner.start();

        // wait for start up to avoid HostsReloader hang
        Thread.sleep(1000);

        final RequestResponseClient client = new RequestResponseClientFactory(testingServer.getConnectString(),
                "/imhotep/shardmasters",
                daemon1).get();

        final ListMultimap<Pair<String, String>, AssignedShard> assignments = FluentIterable.from(Iterables.concat(
                client.getAssignments(daemon1),
                client.getAssignments(daemon2),
                client.getAssignments(daemon3)
        )).index(new Function<AssignedShard, Pair<String, String>>() {
            @Override
            public Pair<String, String> apply(final AssignedShard input) {
                final String shardId = Paths.get(input.getShardPath()).getFileName().toString().split("\\.")[0];
                return Pair.of(input.getDataset(), shardId);
            }
        });

        for (final Collection<AssignedShard> entry : assignments.asMap().values()) {
            Assert.assertEquals(replicationFactor, entry.size());
        }

        Assert.assertEquals(
                Sets.newHashSet(
                        Arrays.asList(
                                Pair.of("dataset1", ShardTimeUtils.toDailyShardPrefix(WEEK_AGO)),
                                Pair.of("dataset1", ShardTimeUtils.toDailyShardPrefix(WEEK_AGO.plusDays(1))),
                                Pair.of("dataset1", ShardTimeUtils.toDailyShardPrefix(WEEK_AGO.plusDays(2))),
                                Pair.of("dataset2", ShardTimeUtils.toDailyShardPrefix(WEEK_AGO)),
                                Pair.of("dataset3", ShardTimeUtils.toDailyShardPrefix(WEEK_AGO)),
                                Pair.of("dataset3", ShardTimeUtils.toDailyShardPrefix(WEEK_AGO.plusDays(1)))
                        )
                ),
                assignments.keySet());

        shardMasterDaemon.shutdown();
        daemonRunner.join();
    }
}