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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
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

import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class H2ShardAssignmentInfoDao implements ShardAssignmentInfoDao {
    private static final Tblshardassignmentinfo TABLE = Tables.TBLSHARDASSIGNMENTINFO;
    private static final int BATCH_SIZE = 100;
    private final DSLContextContainer dslContextContainer;
    private final Duration stalenessThreshold;

    public H2ShardAssignmentInfoDao(final HikariDataSource dataSource, final Duration stalenessThreshold) {
        dslContextContainer = new DSLContextContainer(dataSource);
        this.stalenessThreshold = stalenessThreshold;
    }

    private static ShardAssignmentInfo fromRecord(final String dataset, final String shardPath, final String hostStr) {
        return new ShardAssignmentInfo(
                dataset,
                shardPath,
                Host.valueOf(hostStr)
        );
    }

    @Override
    public Iterable<ShardAssignmentInfo> getAssignments(final Host node) {
        final String hostStr = node.toString();
        final ImmutableSet<ShardAssignmentInfo> shardAssignmentInfos = FluentIterable.from(dslContextContainer.getDSLContext()
                .select(TABLE.DATASET, TABLE.SHARD_PATH)
                .from(TABLE)
                .where(TABLE.ASSIGNED_NODE.eq(hostStr))
                .fetch()).transform(new Function<Record2<String, String>, ShardAssignmentInfo>() {
            @Override
            public ShardAssignmentInfo apply(final Record2<String, String> record) {
                return fromRecord(record.value1(), record.value2(), hostStr);
            }
        }).toSet();

        System.out.println(shardAssignmentInfos);
        return shardAssignmentInfos;
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
    @Override
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

    // TODO: test this
    @Override
    public List<Host> getAssignments(String path) {
        final List<String> fetch = dslContextContainer.getDSLContext().select(TABLE.DATASET, TABLE.SHARD_PATH)
                .from(TABLE)
                .where(TABLE.SHARD_PATH.eq(path))
                .fetch(TABLE.ASSIGNED_NODE);
        return fetch.stream().map(a -> new Host(a.split(":")[0], Integer.valueOf(a.split(":")[1]))).collect(Collectors.toList());
    }
}
