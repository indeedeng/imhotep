package com.indeed.imhotep.shardmanager;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.dbutil.DbDataFixture;
import com.indeed.imhotep.shardmanager.db.shardinfo.Tables;
import com.indeed.imhotep.shardmanager.model.ShardAssignmentInfo;
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

    @Before
    public void setUp() throws IOException, SQLException, URISyntaxException {
        assignmentInfoDao = new ShardAssignmentInfoDao(dbDataFixture.getDataSource());
    }

    @Test
    public void testGetUpdate() {
        Assert.assertTrue(
                FluentIterable.from(assignmentInfoDao.getAssignments("A")).toList().isEmpty()
        );

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

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", 1, "A"),
                        new ShardAssignmentInfo("dataset2", "shard1", 2, "A")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("A")).toSet()
        );

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard2", 2, "B"),
                        new ShardAssignmentInfo("dataset2", "shard2", 3, "B"),
                        new ShardAssignmentInfo("dataset3", "shard1", 5, "B"),
                        new ShardAssignmentInfo("dataset3", "shard3", 6, "B")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("B")).toSet()
        );

        Assert.assertEquals(
                Collections.emptySet(),
                FluentIterable.from(assignmentInfoDao.getAssignments("D")).toSet()
        );

        assignmentInfoDao.updateAssignments("dataset1", Arrays.asList(
                new ShardAssignmentInfo("dataset1", "shard1", 2, "C"),
                new ShardAssignmentInfo("dataset1", "shard1", 2, "B"),
                new ShardAssignmentInfo("dataset1", "shard2", 3, "A"),
                new ShardAssignmentInfo("dataset1", "shard2", 3, "C"),
                new ShardAssignmentInfo("dataset1", "shard3", 2, "B"),
                new ShardAssignmentInfo("dataset1", "shard3", 2, "A")
        ));

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard2", 3, "A"),
                        new ShardAssignmentInfo("dataset1", "shard3", 2, "A"),
                        new ShardAssignmentInfo("dataset2", "shard1", 2, "A")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("A")).toSet()
        );

        Assert.assertEquals(
                ImmutableSet.of(
                        new ShardAssignmentInfo("dataset1", "shard1", 2, "B"),
                        new ShardAssignmentInfo("dataset1", "shard3", 2, "B"),
                        new ShardAssignmentInfo("dataset2", "shard2", 3, "B"),
                        new ShardAssignmentInfo("dataset3", "shard1", 5, "B"),
                        new ShardAssignmentInfo("dataset3", "shard3", 6, "B")
                ),
                FluentIterable.from(assignmentInfoDao.getAssignments("B")).toSet()
        );

    }
}