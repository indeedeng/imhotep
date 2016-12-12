package com.indeed.imhotep.shardmaster;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.dbutil.DbDataFixture;
import com.indeed.imhotep.shardmaster.db.shardinfo.Tables;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author kenh
 */

public class ShardAssignmentInfoDaoTest {
    @Rule
    public final DbDataFixture dbDataFixture = new DbDataFixture(Collections.singletonList(Tables.TBLSHARDASSIGNMENTINFO));
    private List<ShardAssignmentInfoDao> assignmentInfoDaos;

    private static final DateTime NOW = DateTime.now(DateTimeZone.forOffsetHours(-6));
    private static final DateTime LATER = NOW.plusHours(1);

    @Before
    public void setUp() throws IOException, SQLException, URISyntaxException {
        final Duration stalenessThreshold = Duration.standardHours(1);
        assignmentInfoDaos = Arrays.asList(
                new H2ShardAssignmentInfoDao(dbDataFixture.getDataSource(), stalenessThreshold),
                new InMemoryShardAssignmentInfoDao(stalenessThreshold)
        );
    }

    private static ShardAssignmentInfo createAssignmentInfo(final String dataset, final String shardId, final Host node) {
        return new ShardAssignmentInfo(dataset, "/var/imhotep/" + dataset + "/" + shardId, node);
    }

    @Test
    public void testGetUpdate() {
        final Host a = new Host("A", 10);
        final Host b = new Host("A", 20);
        final Host c = new Host("C", 10);
        final Host d = new Host("C", 20);

        for (final ShardAssignmentInfoDao assignmentInfoDao : assignmentInfoDaos) {
            Assert.assertTrue(
                    FluentIterable.from(assignmentInfoDao.getAssignments(a)).toList().isEmpty()
            );

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

            Assert.assertEquals(
                    ImmutableSet.of(
                            createAssignmentInfo("dataset1", "shard1", a),
                            createAssignmentInfo("dataset2", "shard1", a)
                    ),
                    FluentIterable.from(assignmentInfoDao.getAssignments(a)).toSet()
            );

            Assert.assertEquals(
                    ImmutableSet.of(
                            createAssignmentInfo("dataset1", "shard2", b),
                            createAssignmentInfo("dataset2", "shard2", b),
                            createAssignmentInfo("dataset3", "shard1", b),
                            createAssignmentInfo("dataset3", "shard3", b)
                    ),
                    FluentIterable.from(assignmentInfoDao.getAssignments(b)).toSet()
            );

            Assert.assertEquals(
                    Collections.emptySet(),
                    FluentIterable.from(assignmentInfoDao.getAssignments(d)).toSet()
            );

            assignmentInfoDao.updateAssignments("dataset1", NOW.plusMinutes(30), Arrays.asList(
                    createAssignmentInfo("dataset1", "shard1", a),
                    createAssignmentInfo("dataset1", "shard1", b),
                    createAssignmentInfo("dataset1", "shard1", d),
                    createAssignmentInfo("dataset1", "shard2", b),
                    createAssignmentInfo("dataset1", "shard2", d),
                    createAssignmentInfo("dataset1", "shard3", b),
                    createAssignmentInfo("dataset1", "shard3", a)
            ));

            Assert.assertEquals(
                    ImmutableSet.of(
                            createAssignmentInfo("dataset1", "shard1", a),
                            createAssignmentInfo("dataset2", "shard1", a),
                            createAssignmentInfo("dataset1", "shard3", a)
                    ),
                    FluentIterable.from(assignmentInfoDao.getAssignments(a)).toSet()
            );

            Assert.assertEquals(
                    ImmutableSet.of(
                            createAssignmentInfo("dataset1", "shard1", b),
                            createAssignmentInfo("dataset1", "shard2", b),
                            createAssignmentInfo("dataset1", "shard3", b),
                            createAssignmentInfo("dataset2", "shard2", b),
                            createAssignmentInfo("dataset3", "shard1", b),
                            createAssignmentInfo("dataset3", "shard3", b)
                    ),
                    FluentIterable.from(assignmentInfoDao.getAssignments(b)).toSet()
            );

            Assert.assertEquals(
                    ImmutableSet.of(
                            createAssignmentInfo("dataset1", "shard1", d),
                            createAssignmentInfo("dataset1", "shard2", d)
                    ),
                    FluentIterable.from(assignmentInfoDao.getAssignments(d)).toSet()
            );

            assignmentInfoDao.updateAssignments("dataset1", LATER, Collections.<ShardAssignmentInfo>emptyList());
            assignmentInfoDao.updateAssignments("dataset2", LATER, Collections.<ShardAssignmentInfo>emptyList());

            Assert.assertEquals(
                    ImmutableSet.of(
                            createAssignmentInfo("dataset1", "shard1", a),
                            createAssignmentInfo("dataset1", "shard3", a)
                    ),
                    FluentIterable.from(assignmentInfoDao.getAssignments(a)).toSet()
            );

            Assert.assertEquals(
                    ImmutableSet.of(
                            createAssignmentInfo("dataset1", "shard1", b),
                            createAssignmentInfo("dataset1", "shard2", b),
                            createAssignmentInfo("dataset1", "shard3", b),
                            createAssignmentInfo("dataset3", "shard1", b),
                            createAssignmentInfo("dataset3", "shard3", b)
                    ),
                    FluentIterable.from(assignmentInfoDao.getAssignments(b)).toSet()
            );

            assignmentInfoDao.updateAssignments("dataset3", LATER, Collections.<ShardAssignmentInfo>emptyList());

            Assert.assertEquals(
                    ImmutableSet.of(
                            createAssignmentInfo("dataset1", "shard1", a),
                            createAssignmentInfo("dataset1", "shard3", a)
                    ),
                    FluentIterable.from(assignmentInfoDao.getAssignments(a)).toSet()
            );

            Assert.assertEquals(
                    ImmutableSet.of(
                            createAssignmentInfo("dataset1", "shard1", b),
                            createAssignmentInfo("dataset1", "shard2", b),
                            createAssignmentInfo("dataset1", "shard3", b)
                    ),
                    FluentIterable.from(assignmentInfoDao.getAssignments(b)).toSet()
            );
        }
    }
}