package com.indeed.imhotep.shardmanager.rpc;

import com.google.common.collect.Sets;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.dbutil.DbDataFixture;
import com.indeed.imhotep.shardmanager.ShardAssignmentInfoDao;
import com.indeed.imhotep.shardmanager.ShardManagerServer;
import com.indeed.imhotep.shardmanager.db.shardinfo.Tables;
import com.indeed.imhotep.shardmanager.model.ShardAssignmentInfo;
import com.indeed.imhotep.shardmanager.protobuf.AssignedShard;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * @author kenh
 */

public class RequestResponseServerTest {
    @Rule
    public final DbDataFixture dbDataFixture = new DbDataFixture(Collections.singletonList(Tables.TBLSHARDASSIGNMENTINFO));
    private ShardAssignmentInfoDao assignmentInfoDao;

    @Before
    public void setUp() {
        assignmentInfoDao = new ShardAssignmentInfoDao(dbDataFixture.getDataSource());
    }

    @Test
    public void testRequestResponse() throws IOException {
        assignmentInfoDao.updateAssignments("dataset1", Arrays.asList(
                new ShardAssignmentInfo("dataset1", "shard1", 1, "A"),
                new ShardAssignmentInfo("dataset1", "shard2", 2, "B"),
                new ShardAssignmentInfo("dataset1", "shard3", 1, "C")
        ));

        assignmentInfoDao.updateAssignments("dataset2", Arrays.asList(
                new ShardAssignmentInfo("dataset2", "shard1", 2, "A"),
                new ShardAssignmentInfo("dataset2", "shard2", 3, "B"),
                new ShardAssignmentInfo("dataset2", "shard3", 4, "C")
        ));

        assignmentInfoDao.updateAssignments("dataset3", Arrays.asList(
                new ShardAssignmentInfo("dataset3", "shard1", 5, "B"),
                new ShardAssignmentInfo("dataset3", "shard2", 5, "C"),
                new ShardAssignmentInfo("dataset3", "shard3", 6, "B")
        ));

        final ShardManagerServer shardManagerServer = new ShardManagerServer(assignmentInfoDao);
        final RequestResponseClient requestResponseClient;

        try (RequestResponseServer requestResponseServer = new RequestResponseServer(
                0, new MultiplexingRequestHandler(shardManagerServer))) {

            requestResponseClient = new RequestResponseClient(
                    new Host("localhost", requestResponseServer.getActualPort()));

            Executors.newSingleThreadExecutor().submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    requestResponseServer.run();
                    return null;
                }
            });

            Assert.assertEquals(
                    Sets.newHashSet(
                            AssignedShard.newBuilder().setDataset("dataset1").setShardId("shard1").setVersion(1).build(),
                            AssignedShard.newBuilder().setDataset("dataset2").setShardId("shard1").setVersion(2).build()
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("A"))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            AssignedShard.newBuilder().setDataset("dataset1").setShardId("shard2").setVersion(2).build(),
                            AssignedShard.newBuilder().setDataset("dataset2").setShardId("shard2").setVersion(3).build(),
                            AssignedShard.newBuilder().setDataset("dataset3").setShardId("shard1").setVersion(5).build(),
                            AssignedShard.newBuilder().setDataset("dataset3").setShardId("shard3").setVersion(6).build()
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("B"))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            AssignedShard.newBuilder().setDataset("dataset1").setShardId("shard3").setVersion(1).build(),
                            AssignedShard.newBuilder().setDataset("dataset2").setShardId("shard3").setVersion(4).build(),
                            AssignedShard.newBuilder().setDataset("dataset3").setShardId("shard2").setVersion(5).build()
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("C"))
            );

            Assert.assertEquals(
                    Collections.emptyList(),
                    requestResponseClient.getAssignments("D")
            );

            assignmentInfoDao.updateAssignments("dataset1", Arrays.asList(
                    new ShardAssignmentInfo("dataset1", "shard1", 2, "A"),
                    new ShardAssignmentInfo("dataset1", "shard1", 2, "B"),
                    new ShardAssignmentInfo("dataset1", "shard2", 3, "C"),
                    new ShardAssignmentInfo("dataset1", "shard2", 3, "A"),
                    new ShardAssignmentInfo("dataset1", "shard3", 2, "B"),
                    new ShardAssignmentInfo("dataset1", "shard3", 2, "C")
            ));

            assignmentInfoDao.updateAssignments("dataset2", Collections.<ShardAssignmentInfo>emptyList());

            assignmentInfoDao.updateAssignments("dataset3", Arrays.asList(
                    new ShardAssignmentInfo("dataset3", "shard1", 6, "A"),
                    new ShardAssignmentInfo("dataset3", "shard1", 6, "B"),
                    new ShardAssignmentInfo("dataset3", "shard2", 6, "A"),
                    new ShardAssignmentInfo("dataset3", "shard2", 6, "B"),
                    new ShardAssignmentInfo("dataset3", "shard3", 7, "A"),
                    new ShardAssignmentInfo("dataset3", "shard3", 7, "B")
            ));

            Assert.assertEquals(
                    Sets.newHashSet(
                            AssignedShard.newBuilder().setDataset("dataset1").setShardId("shard1").setVersion(2).build(),
                            AssignedShard.newBuilder().setDataset("dataset1").setShardId("shard2").setVersion(3).build(),
                            AssignedShard.newBuilder().setDataset("dataset3").setShardId("shard1").setVersion(6).build(),
                            AssignedShard.newBuilder().setDataset("dataset3").setShardId("shard2").setVersion(6).build(),
                            AssignedShard.newBuilder().setDataset("dataset3").setShardId("shard3").setVersion(7).build()
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("A"))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            AssignedShard.newBuilder().setDataset("dataset1").setShardId("shard1").setVersion(2).build(),
                            AssignedShard.newBuilder().setDataset("dataset1").setShardId("shard3").setVersion(2).build(),
                            AssignedShard.newBuilder().setDataset("dataset3").setShardId("shard1").setVersion(6).build(),
                            AssignedShard.newBuilder().setDataset("dataset3").setShardId("shard2").setVersion(6).build(),
                            AssignedShard.newBuilder().setDataset("dataset3").setShardId("shard3").setVersion(7).build()
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("B"))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            AssignedShard.newBuilder().setDataset("dataset1").setShardId("shard2").setVersion(3).build(),
                            AssignedShard.newBuilder().setDataset("dataset1").setShardId("shard3").setVersion(2).build()
                            ),
                    Sets.newHashSet(requestResponseClient.getAssignments("C"))
            );
        }
     }
}