package com.indeed.imhotep.shardmaster.rpc;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.dbutil.DbDataFixture;
import com.indeed.imhotep.shardmaster.DatabaseShardMaster;
import com.indeed.imhotep.shardmaster.ShardAssignmentInfoDao;
import com.indeed.imhotep.shardmaster.db.shardinfo.Tables;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author kenh
 */

public class RequestResponseServerTest {
    @Rule
    public final DbDataFixture dbDataFixture = new DbDataFixture(Collections.singletonList(Tables.TBLSHARDASSIGNMENTINFO));
    private ShardAssignmentInfoDao assignmentInfoDao;
    private static final DateTime NOW = DateTime.now(DateTimeZone.forOffsetHours(-6));
    private static final DateTime LATER = NOW.plusHours(1);

    @Before
    public void setUp() {
        assignmentInfoDao = new ShardAssignmentInfoDao(dbDataFixture.getDataSource(), Duration.standardHours(1));
    }

    private static ShardAssignmentInfo createAssignmentInfo(final String dataset, final String shardId, final String node) {
        return new ShardAssignmentInfo(dataset, "/var/imhotep/" + dataset + "/" + shardId, node);
    }

    private static AssignedShard createAssignedShard(final String dataset, final String shardId) {
        return AssignedShard.newBuilder().setDataset(dataset)
                .setShardPath("/var/imhotep/" + dataset + "/" + shardId).build();
    }

    @Test
    public void testRequestResponse() throws IOException {
        assignmentInfoDao.updateAssignments("dataset1", NOW, Arrays.asList(
                createAssignmentInfo("dataset1", "shard1", "A"),
                createAssignmentInfo("dataset1", "shard2", "B"),
                createAssignmentInfo("dataset1", "shard3", "C")
        ));

        assignmentInfoDao.updateAssignments("dataset2", NOW, Arrays.asList(
                createAssignmentInfo("dataset2", "shard1", "A"),
                createAssignmentInfo("dataset2", "shard2", "B"),
                createAssignmentInfo("dataset2", "shard3", "C")
        ));

        assignmentInfoDao.updateAssignments("dataset3", NOW, Arrays.asList(
                createAssignmentInfo("dataset3", "shard1", "B"),
                createAssignmentInfo("dataset3", "shard2", "C"),
                createAssignmentInfo("dataset3", "shard3", "B")
        ));

        final DatabaseShardMaster shardMasterServer = new DatabaseShardMaster(assignmentInfoDao);
        final RequestResponseClient requestResponseClient;

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        try (RequestResponseServer requestResponseServer = new RequestResponseServer(
                0, new MultiplexingRequestHandler(shardMasterServer, 2), 2)) {

            requestResponseClient = new RequestResponseClient(
                    new Host("localhost", requestResponseServer.getActualPort()));

            executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    requestResponseServer.run();
                    return null;
                }
            });

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard1"),
                            createAssignedShard("dataset2", "shard1")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("A"))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard2"),
                            createAssignedShard("dataset2", "shard2"),
                            createAssignedShard("dataset3", "shard1"),
                            createAssignedShard("dataset3", "shard3")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("B"))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard3"),
                            createAssignedShard("dataset2", "shard3"),
                            createAssignedShard("dataset3", "shard2")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("C"))
            );

            Assert.assertEquals(
                    Collections.emptyList(),
                    Lists.newArrayList(requestResponseClient.getAssignments("D"))
            );

            assignmentInfoDao.updateAssignments("dataset1", LATER, Arrays.asList(
                    createAssignmentInfo("dataset1", "shard1", "A"),
                    createAssignmentInfo("dataset1", "shard1", "B"),
                    createAssignmentInfo("dataset1", "shard2", "C"),
                    createAssignmentInfo("dataset1", "shard2", "A"),
                    createAssignmentInfo("dataset1", "shard3", "B"),
                    createAssignmentInfo("dataset1", "shard3", "C")
            ));

            assignmentInfoDao.updateAssignments("dataset2", LATER, Collections.<ShardAssignmentInfo>emptyList());

            assignmentInfoDao.updateAssignments("dataset3", LATER, Arrays.asList(
                    createAssignmentInfo("dataset3", "shard1", "A"),
                    createAssignmentInfo("dataset3", "shard1", "B"),
                    createAssignmentInfo("dataset3", "shard2", "A"),
                    createAssignmentInfo("dataset3", "shard2", "B"),
                    createAssignmentInfo("dataset3", "shard3", "A"),
                    createAssignmentInfo("dataset3", "shard3", "B")
            ));

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard1"),
                            createAssignedShard("dataset1", "shard2"),
                            createAssignedShard("dataset3", "shard1"),
                            createAssignedShard("dataset3", "shard2"),
                            createAssignedShard("dataset3", "shard3")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("A"))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard1"),
                            createAssignedShard("dataset1", "shard3"),
                            createAssignedShard("dataset3", "shard1"),
                            createAssignedShard("dataset3", "shard2"),
                            createAssignedShard("dataset3", "shard3")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("B"))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard2"),
                            createAssignedShard("dataset1", "shard3")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments("C"))
            );
        }

        executorService.shutdownNow();
    }
}