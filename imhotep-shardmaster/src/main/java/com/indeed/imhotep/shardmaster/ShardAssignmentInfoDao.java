package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.fs.sql.DSLContextContainer;
import com.indeed.imhotep.shardmaster.db.shardinfo.Tables;
import com.indeed.imhotep.shardmaster.db.shardinfo.tables.Tblshardassignmentinfo;
import com.indeed.imhotep.shardmaster.db.shardinfo.tables.records.TblshardassignmentinfoRecord;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import com.zaxxer.hikari.HikariDataSource;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.jooq.BatchBindStep;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.TransactionalRunnable;
import org.jooq.impl.DSL;

import java.sql.Timestamp;

/**
 * @author kenh
 */

public class ShardAssignmentInfoDao {
    private static final Tblshardassignmentinfo TABLE = Tables.TBLSHARDASSIGNMENTINFO;
    private static final int BATCH_SIZE = 1000;
    private final DSLContext dslContext;
    private final Duration stalenessThreshold;

    public ShardAssignmentInfoDao(final HikariDataSource dataSource, final Duration stalenessThreshold) {
        dslContext = new DSLContextContainer(dataSource).getDSLContext();
        this.stalenessThreshold = stalenessThreshold;
    }

    private static ShardAssignmentInfo fromRecord(final TblshardassignmentinfoRecord record) {
        return new ShardAssignmentInfo(
                record.getDataset(),
                record.getShardId(),
                record.getShardPath(),
                record.getAssignedNode()
        );
    }

    Iterable<ShardAssignmentInfo> getAssignments(final String node) {
        return FluentIterable.from(dslContext.selectFrom(TABLE)
                .where(TABLE.ASSIGNED_NODE.eq(node))
                .fetch()).transform(new Function<TblshardassignmentinfoRecord, ShardAssignmentInfo>() {
            @Override
            public ShardAssignmentInfo apply(final TblshardassignmentinfoRecord record) {
                return fromRecord(record);
            }
        }).toSet();
    }

    private static BatchBindStep createInsertBatch(final DSLContext dslContext) {
        return dslContext.batch(
                dslContext.insertInto(TABLE,
                        TABLE.DATASET,
                        TABLE.SHARD_ID,
                        TABLE.SHARD_PATH,
                        TABLE.ASSIGNED_NODE,
                        TABLE.TIMESTAMP
                )
                        .values((String) null, null, null, null, null)
        );
    }

    public void updateAssignments(final String dataset, final DateTime timestamp, final Iterable<ShardAssignmentInfo> assignmentInfos) {
        dslContext.transaction(new TransactionalRunnable() {
            @Override
            public void run(final Configuration configuration) throws Exception {
                final DSLContext txnDslContext = DSL.using(configuration);

                txnDslContext.deleteFrom(TABLE)
                        .where(TABLE.DATASET.eq(dataset)
                                .and(TABLE.TIMESTAMP.le(new Timestamp(timestamp.minus(stalenessThreshold).getMillis()))))
                        .execute();

                BatchBindStep insertBatch = createInsertBatch(txnDslContext);
                for (final ShardAssignmentInfo assignmentInfo : assignmentInfos) {
                    insertBatch.bind(
                            assignmentInfo.getDataset(),
                            assignmentInfo.getShardId(),
                            assignmentInfo.getShardPath(),
                            assignmentInfo.getAssignedNode(),
                            timestamp.getMillis()
                    );

                    if (insertBatch.size() > BATCH_SIZE) {
                        insertBatch.execute();
                        insertBatch = createInsertBatch(txnDslContext);
                    }
                }

                if (insertBatch.size() > 0) {
                    insertBatch.execute();
                }
            }
        });
    }
}
