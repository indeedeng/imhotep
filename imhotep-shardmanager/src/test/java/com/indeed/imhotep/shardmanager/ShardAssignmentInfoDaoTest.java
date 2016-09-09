package com.indeed.imhotep.shardmanager;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.dbutil.DbDataFixture;
import com.indeed.imhotep.shardmanager.db.shardinfo.Tables;
import com.indeed.imhotep.shardmanager.model.ShardAssignmentInfo;
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

/**
 * @author kenh
 */

public class ShardAssignmentInfoDaoTest {
    @Rule
    public final DbDataFixture dbDataFixture = new DbDataFixture(Collections.singletonList(Tables.TBLSHARDASSIGNMENTINFO));
    private ShardAssignmentInfoDao assignmentInfoDao;

    private static final DateTime NOW = DateTime.now(DateTimeZone.forOffsetHours(-6));
    private static final DateTime LATER = NOW.plusHours(1);

    @Before
    public void setUp() throws IOException, SQLException, URISyntaxException {
        assignmentInfoDao = new ShardAssignmentInfoDao(dbDataFixture.getDataSource(), Duration.standardHours(1));
    }

    @Test
    public void testGetUpdate() {
        Assert.assertTrue(
                FluentIterable.from(assignmentInfoDao.getAssignments("A")).toList().isEmpty()
        );

        assignmentInfoDao.updateAssignments("dataset1", NOW, Arrays.asList(
                new ShardAssignmentInfo("dataset1", "shard1", "A"),
                new ShardAssignmentInfo("dataset1", "shard2", "B"),
                new ShardAssignmentInfo("dataset1", "shard3", "C")
        ));

        assignmentInfoDao.updateAssignments("dataset2", NOW, Arrays.asList(
                new ShardAssignmentInfo("dataset2", "shard1", "A"),
                new ShardAssignmentInfo("dataset2", "shard2", "B"),
                new ShardAssignmentInfo("dataset2", "shard3", "C")
        ));

        assignmentInfoDao.updateAssignments("dataset3", NOW, Arrays.asList(
                new ShardAssignmentInfo("dataset3", "shard1", "B"),
                new ShardAssignmentInfo("dataset3", "shard2", "C"),
                new ShardAssignmentInfo("dataset3", "shard3", "B")
        ));

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", "A"),
                        new ShardAssignmentInfo("dataset2", "shard1", "A")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("A")).toSet()
        );

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard2", "B"),
                        new ShardAssignmentInfo("dataset2", "shard2", "B"),
                        new ShardAssignmentInfo("dataset3", "shard1", "B"),
                        new ShardAssignmentInfo("dataset3", "shard3", "B")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("B")).toSet()
        );

        Assert.assertEquals(
                Collections.emptySet(),
                FluentIterable.from(assignmentInfoDao.getAssignments("D")).toSet()
        );

        assignmentInfoDao.updateAssignments("dataset1", NOW.plusMinutes(30), Arrays.asList(
                new ShardAssignmentInfo("dataset1", "shard1", "A"),
                new ShardAssignmentInfo("dataset1", "shard1", "B"),
                new ShardAssignmentInfo("dataset1", "shard1", "D"),
                new ShardAssignmentInfo("dataset1", "shard2", "B"),
                new ShardAssignmentInfo("dataset1", "shard2", "D"),
                new ShardAssignmentInfo("dataset1", "shard3", "B"),
                new ShardAssignmentInfo("dataset1", "shard3", "A")
        ));

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", "A"),
                        new ShardAssignmentInfo("dataset2", "shard1", "A"),
                        new ShardAssignmentInfo("dataset1", "shard3", "A")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("A")).toSet()
        );

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", "B"),
                        new ShardAssignmentInfo("dataset1", "shard2", "B"),
                        new ShardAssignmentInfo("dataset1", "shard3", "B"),
                        new ShardAssignmentInfo("dataset2", "shard2", "B"),
                        new ShardAssignmentInfo("dataset3", "shard1", "B"),
                        new ShardAssignmentInfo("dataset3", "shard3", "B")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("B")).toSet()
        );

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", "D"),
                        new ShardAssignmentInfo("dataset1", "shard2", "D")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("D")).toSet()
        );

        assignmentInfoDao.updateAssignments("dataset1", LATER, Collections.<ShardAssignmentInfo>emptyList());
        assignmentInfoDao.updateAssignments("dataset2", LATER, Collections.<ShardAssignmentInfo>emptyList());

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", "A"),
                        new ShardAssignmentInfo("dataset1", "shard3", "A")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("A")).toSet()
        );

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", "B"),
                        new ShardAssignmentInfo("dataset1", "shard2", "B"),
                        new ShardAssignmentInfo("dataset1", "shard3", "B"),
                        new ShardAssignmentInfo("dataset3", "shard1", "B"),
                        new ShardAssignmentInfo("dataset3", "shard3", "B")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("B")).toSet()
        );

        assignmentInfoDao.updateAssignments("dataset3", LATER, Collections.<ShardAssignmentInfo>emptyList());

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", "A"),
                        new ShardAssignmentInfo("dataset1", "shard3", "A")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("A")).toSet()
        );

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", "B"),
                        new ShardAssignmentInfo("dataset1", "shard2", "B"),
                        new ShardAssignmentInfo("dataset1", "shard3", "B")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("B")).toSet()
        );

    }
}