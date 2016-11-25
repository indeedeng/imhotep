package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.fs.sql.DSLContextContainer;
import com.indeed.imhotep.shardmaster.db.shardinfo.Tables;
import com.indeed.imhotep.shardmaster.db.shardinfo.tables.Tblshardassignmentinfo;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import com.zaxxer.hikari.HikariDataSource;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Record2;

import java.sql.Timestamp;

/**
 * @author kenh
 */

public class ShardAssignmentInfoDao {
    private static final Tblshardassignmentinfo TABLE = Tables.TBLSHARDASSIGNMENTINFO;
    private static final int BATCH_SIZE = 100;
    private final DSLContextContainer dslContextContainer;
    private final Duration stalenessThreshold;

    public ShardAssignmentInfoDao(final HikariDataSource dataSource, final Duration stalenessThreshold) {
        dslContextContainer = new DSLContextContainer(dataSource);
        this.stalenessThreshold = stalenessThreshold;
    }

    private static ShardAssignmentInfo fromRecord(final String dataset, final String shardPath, final Host assignedNode) {
        return new ShardAssignmentInfo(
                dataset,
                shardPath,
                assignedNode
        );
    }

    Iterable<ShardAssignmentInfo> getAssignments(final Host node) {
        return FluentIterable.from(dslContextContainer.getDSLContext()
                .select(TABLE.DATASET, TABLE.SHARD_PATH)
                .from(TABLE)
                .where(TABLE.ASSIGNED_NODE.eq(node.toString()))
                .fetch()).transform(new Function<Record2<String, String>, ShardAssignmentInfo>() {
            @Override
            public ShardAssignmentInfo apply(final Record2<String, String> record) {
                return fromRecord(record.value1(), record.value2(), node);
            }
        }).toSet();
    }

    private static BatchBindStep createInsertBatch(final DSLContext dslContext) {
        return dslContext.batch(
                dslContext.insertInto(TABLE,
                        TABLE.DATASET,
                        TABLE.SHARD_PATH,
                        TABLE.ASSIGNED_NODE,
                        TABLE.TIMESTAMP
                )
                        .values((String) null, null, null, null)
        );
    }

    /**
     * updates the shard assignments for a given dataset.
     * This flushes out the old assignment, but with some time delay.
     * The reason for this is because during host fluctuation, there could be a transient state where a given
     * shard is allocated to no hosts, so to stay conservative, we retain old shard assignments for some time.
     */
    public void updateAssignments(final String dataset, final DateTime timestamp, final Iterable<ShardAssignmentInfo> assignmentInfos) {
        final DSLContext dslContext = dslContextContainer.getDSLContext();

        BatchBindStep insertBatch = createInsertBatch(dslContext);
        for (final ShardAssignmentInfo assignmentInfo : assignmentInfos) {
            insertBatch.bind(
                    assignmentInfo.getDataset(),
                    assignmentInfo.getShardPath(),
                    assignmentInfo.getAssignedNode().toString(),
                    timestamp.getMillis()
            );

            if (insertBatch.size() > BATCH_SIZE) {
                insertBatch.execute();
                insertBatch = createInsertBatch(dslContext);
            }
        }

        if (insertBatch.size() > 0) {
            insertBatch.execute();
        }

        dslContext.deleteFrom(TABLE)
                .where(TABLE.DATASET.eq(dataset)
                        .and(TABLE.TIMESTAMP.le(new Timestamp(timestamp.minus(stalenessThreshold).getMillis()))))
                .execute();

    }
}
