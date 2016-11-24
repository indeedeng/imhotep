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
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
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

    private static ShardAssignmentInfo createAssignmentInfo(final String dataset, final String shardId, final Host host) {
        return new ShardAssignmentInfo(dataset, "/var/imhotep/" + dataset + "/" + shardId, host);
    }

    private static AssignedShard createAssignedShard(final String dataset, final String shardId) {
        return AssignedShard.newBuilder().setDataset(dataset)
                .setShardPath("/var/imhotep/" + dataset + "/" + shardId).build();
    }

    static class CountingRequestStatsEmitter implements RequestMetricStatsEmitter {
        private int processed = 0;

        @Override
        public synchronized void processed(final String requestKey, final ShardMasterRequest.RequestType requestType, final long millis) {
            ++processed;
        }

        public synchronized int getProcessed() {
            return processed;
        }
    }

    @Test
    public void testRequestResponse() throws IOException {
        final Host a = new Host("A", 10);
        final Host b = new Host("A", 20);
        final Host c = new Host("C", 10);
        final Host d = new Host("C", 20);

        assignmentInfoDao.updateAssignments("dataset1", NOW, Arrays.asList(
                createAssignmentInfo("dataset1", "shard1", a),
                createAssignmentInfo("dataset1", "shard2", b),
                createAssignmentInfo("dataset1", "shard3", c)
        ));

        assignmentInfoDao.updateAssignments("dataset2", NOW, Arrays.asList(
                createAssignmentInfo("dataset2", "shard1", a),
                createAssignmentInfo("dataset2", "shard2", b),
                createAssignmentInfo("dataset2", "shard3", c)
        ));

        assignmentInfoDao.updateAssignments("dataset3", NOW, Arrays.asList(
                createAssignmentInfo("dataset3", "shard1", b),
                createAssignmentInfo("dataset3", "shard2", c),
                createAssignmentInfo("dataset3", "shard3", b)
        ));

        final DatabaseShardMaster shardMasterServer = new DatabaseShardMaster(assignmentInfoDao);
        final RequestResponseClient requestResponseClient;

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        final CountingRequestStatsEmitter statsEmitter = new CountingRequestStatsEmitter();

        try (RequestResponseServer requestResponseServer = new RequestResponseServer(
                0, new MultiplexingRequestHandler(statsEmitter, shardMasterServer, 2), 2)) {

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
                    Sets.newHashSet(requestResponseClient.getAssignments(a))
            );

            Assert.assertEquals(1, statsEmitter.getProcessed());

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard2"),
                            createAssignedShard("dataset2", "shard2"),
                            createAssignedShard("dataset3", "shard1"),
                            createAssignedShard("dataset3", "shard3")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments(b))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard3"),
                            createAssignedShard("dataset2", "shard3"),
                            createAssignedShard("dataset3", "shard2")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments(c))
            );

            Assert.assertEquals(
                    Collections.emptyList(),
                    Lists.newArrayList(requestResponseClient.getAssignments(d))
            );

            Assert.assertEquals(4, statsEmitter.getProcessed());

            assignmentInfoDao.updateAssignments("dataset1", LATER, Arrays.asList(
                    createAssignmentInfo("dataset1", "shard1", a),
                    createAssignmentInfo("dataset1", "shard1", b),
                    createAssignmentInfo("dataset1", "shard2", c),
                    createAssignmentInfo("dataset1", "shard2", a),
                    createAssignmentInfo("dataset1", "shard3", b),
                    createAssignmentInfo("dataset1", "shard3", c)
            ));

            assignmentInfoDao.updateAssignments("dataset2", LATER, Collections.<ShardAssignmentInfo>emptyList());

            assignmentInfoDao.updateAssignments("dataset3", LATER, Arrays.asList(
                    createAssignmentInfo("dataset3", "shard1", a),
                    createAssignmentInfo("dataset3", "shard1", b),
                    createAssignmentInfo("dataset3", "shard2", a),
                    createAssignmentInfo("dataset3", "shard2", b),
                    createAssignmentInfo("dataset3", "shard3", a),
                    createAssignmentInfo("dataset3", "shard3", b)
            ));

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard1"),
                            createAssignedShard("dataset1", "shard2"),
                            createAssignedShard("dataset3", "shard1"),
                            createAssignedShard("dataset3", "shard2"),
                            createAssignedShard("dataset3", "shard3")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments(a))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard1"),
                            createAssignedShard("dataset1", "shard3"),
                            createAssignedShard("dataset3", "shard1"),
                            createAssignedShard("dataset3", "shard2"),
                            createAssignedShard("dataset3", "shard3")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments(b))
            );

            Assert.assertEquals(
                    Sets.newHashSet(
                            createAssignedShard("dataset1", "shard2"),
                            createAssignedShard("dataset1", "shard3")
                    ),
                    Sets.newHashSet(requestResponseClient.getAssignments(c))
            );
        }

        Assert.assertEquals(7, statsEmitter.getProcessed());

        executorService.shutdownNow();
    }
}