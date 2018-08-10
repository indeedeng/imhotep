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

import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.shardmaster.utils.SQLWriteManager;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * @author kenh
 */

public class DatasetShardRefresher {
    private static final Logger LOGGER = Logger.getLogger(DatasetShardRefresher.class);
    private final Path datasetsDir;
    private static final ExecutorService executorService = new ForkJoinPool(40);
    private final JdbcTemplate dbConnection;
    private final org.apache.hadoop.fs.FileSystem hadoopFileSystem;
    private final ShardFilter filter;
    private final ShardData shardData;
    private Timestamp lastUpdatedTimestamp;
    private final SQLWriteManager sqlWriteManager;
    private final AtomicBoolean setup;



    DatasetShardRefresher(final Path datasetsDir,
                          final JdbcTemplate dbConnection,
                          final String rootURI,
                          final ShardFilter filter,
                          final ShardData shardData,
                          final SQLWriteManager manager,
                          final AtomicBoolean setup) throws IOException {
        this.datasetsDir = datasetsDir;
        this.dbConnection = dbConnection;
        this.hadoopFileSystem = new Path(rootURI).getFileSystem(new Configuration());
        this.filter = filter;
        this.shardData = shardData;
        this.sqlWriteManager = manager;
        this.lastUpdatedTimestamp = Timestamp.from(Instant.MIN);
        this.setup = setup;
    }

    private void loadFromSQL(boolean shouldDelete) {
        final Timestamp timestampToUse = lastUpdatedTimestamp;
        lastUpdatedTimestamp = Timestamp.from(Instant.now());
        if(shouldDelete){
            dbConnection.query("SELECT * FROM tblshards;", (ResultSetExtractor<Void>) rs -> {
                shardData.updateTableShardsRowsFromSQL(rs, true);
                return null;
            });
        } else {
            dbConnection.query("SELECT * FROM tblshards WHERE addedtimestamp >= ?;", statement -> statement.setTimestamp(1, timestampToUse), (ResultSetExtractor<Void>) rs -> {
                shardData.updateTableShardsRowsFromSQL(rs, false);
                return null;
            });
        }

        dbConnection.query("SELECT * FROM tblfields;", (ResultSetExtractor<ResultSet>) rs -> {
            shardData.updateTableFieldsRowsFromSQL(rs);
            return null;
        });
    }

    private void innerRun(boolean leader, boolean shouldDeleteUsingSQL) {
        loadFromSQL(shouldDeleteUsingSQL);

        if(!leader) {
            return;
        }

        DataSetScanner scanner = new DataSetScanner(datasetsDir, hadoopFileSystem);
        try {
            final Set<String> allRemaining = shardData.getCopyOfAllPaths();
            executorService.submit(() -> StreamSupport.stream(scanner.spliterator(), true).filter(path -> filter.accept(path.getName())).forEach(a -> handleDataset(a, allRemaining))).get();

            deleteFromSQL(new ArrayList<>(allRemaining));
            shardData.deleteShards(allRemaining);
            List<String> deletedDatasets = shardData.deleteDatasetsWithoutShards();
            deleteDatasetsFromSQL(deletedDatasets);

        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void deleteDatasetsFromSQL(List<String> deletedDatasets) {
        Runnable run = () -> {
            dbConnection.batchUpdate("DELETE FROM tblfields WHERE dataset = ?;", new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setString(1, deletedDatasets.get(i));
                }

                @Override
                public int getBatchSize() {
                    return deletedDatasets.size();
                }
            });
        };

        sqlWriteManager.addStatementToQueue(run);
        sqlWriteManager.run();
    }


    private void handleDataset(final Path path, final Set<String> allPaths) {
        LOGGER.info("On dataset: " + path);
        Iterable<ShardDir> dataset = new ShardScanner(path, hadoopFileSystem);

        List<Pair<ShardDir, Future<FlamdexMetadata>>> pairs = new ArrayList<>();

        for(final ShardDir dir: dataset) {
            allPaths.remove(dir.getIndexDir().toString());
            if(filter.accept(dir.getDataset(), dir.getId()) && isValidAndNew(dir)) {
                pairs.add(new Pair<>(dir, executorService.submit(() -> collectShardMetadata(dir))));
            }
        }

        for(final Pair<ShardDir, Future<FlamdexMetadata>> p: pairs) {
            try {
                if(p.getValue().get() == null) {
                    LOGGER.error("metadata was null for shard " + p.getKey());
                }
                addData(p.getKey(), p.getValue().get());
            } catch (final SQLException e) {
                LOGGER.error("could not add data to SQL", e);
            } catch (final ExecutionException | InterruptedException e) {
                LOGGER.error("error with getting metadata", e);
            }
        }
    }

    private boolean isValidAndNew(final ShardDir temp) {
        final String dataset = temp.getDataset();
        final String id = temp.getId();
        return !shardData.hasShard(dataset + "/" + temp.getName()) && ShardTimeUtils.isValidShardId(id);
    }

    private FlamdexMetadata collectShardMetadata(final ShardDir shardDir) throws IOException {
        LOGGER.info("reading a shard with dataset: " + shardDir.getDataset() + " and id: " + shardDir.getId() + " and version " + shardDir.getVersion());
        FlamdexMetadata metadata = FlamdexMetadata.readMetadata(hadoopFileSystem, shardDir.getHadoopPath());
        LOGGER.info("finished reading a shard with dataset: " + shardDir.getDataset() + " and id: " + shardDir.getId() + " and version " + shardDir.getVersion());
        return metadata;
    }

    private void addData(final ShardDir shardDir, final FlamdexMetadata metadata) throws SQLException {
        addToSQL(metadata, shardDir);
        shardData.addShardFromHDFS(metadata, shardDir);
    }

    private void addToSQL(final FlamdexMetadata metadata, final ShardDir shardDir) throws SQLException {
        final String shardId = shardDir.getId();
        final String dataset = shardDir.getDataset();
        Runnable tblShardsInsertStatement = () -> dbConnection.update("INSERT INTO tblshards (path, numDocs) VALUES (?, ?);", statement -> {
            statement.setString(1, shardDir.getIndexDir().toString());
            statement.setInt(2, metadata.getNumDocs());
        });

        final long startTime = ShardTimeUtils.parseStart(shardId).getMillis();

        final List<String> stringFieldsForInsert = metadata.getStringFields().stream().filter(field -> (!shardData.hasField(dataset, field))).collect(Collectors.toList());
        final List<String> intFieldsForInsert = metadata.getIntFields().stream().filter(field -> (!shardData.hasField(dataset, field))).collect(Collectors.toList());
        final List<String> stringFielsForUpdate = metadata.getStringFields().stream().filter(field -> shardData.hasField(dataset, field) && shardData.getFieldUpdateTime(dataset, field) < startTime).collect(Collectors.toList());
        final List<String> intFielsForUpdate = metadata.getIntFields().stream().filter(field -> shardData.hasField(dataset, field) && shardData.getFieldUpdateTime(dataset, field) < startTime).collect(Collectors.toList());


        Runnable tblFieldsStringInsertStatement = () -> dbConnection.batchUpdate("INSERT INTO tblfields (dataset, fieldname, type, lastshardstarttime) VALUES (?, ?, ?, ?);", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, dataset);
                ps.setString(2, stringFieldsForInsert.get(i));
                ps.setString(3, "STRING");
                ps.setLong(4, startTime);
            }

            @Override
            public int getBatchSize() {
                return stringFieldsForInsert.size();
            }
        });

        Runnable tblFieldsIntInsertStatement = () -> dbConnection.batchUpdate("INSERT INTO tblfields (dataset, fieldname, type, lastshardstarttime) VALUES (?, ?, ?, ?);", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, dataset);
                ps.setString(2, intFieldsForInsert.get(i));
                ps.setString(3, "INT");
                ps.setLong(4, startTime);
            }

            @Override
            public int getBatchSize() {
                return intFieldsForInsert.size();
            }
        });

        Runnable tblFieldsStringUpdateStatement = () -> dbConnection.batchUpdate("UPDATE tblfields SET type = ?, lastshardstarttime = ? WHERE dataset = ? AND fieldname = ?;", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, "STRING");
                ps.setLong(2, startTime);
                ps.setString(3, dataset);
                ps.setString(4, stringFielsForUpdate.get(i));
            }

            @Override
            public int getBatchSize() {
                return stringFielsForUpdate.size();
            }
        });

        Runnable tblFieldsIntUpdateStatement = () -> dbConnection.batchUpdate("UPDATE tblfields SET type = ?, lastshardstarttime = ? WHERE dataset = ? AND fieldname = ?;", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, "INT");
                ps.setLong(2, startTime);
                ps.setString(3, dataset);
                ps.setString(4, intFielsForUpdate.get(i));
            }

            @Override
            public int getBatchSize() {
                return intFielsForUpdate.size();
            }
        });

        sqlWriteManager.addStatementToQueue(tblFieldsStringInsertStatement);
        sqlWriteManager.addStatementToQueue(tblFieldsIntInsertStatement);
        sqlWriteManager.addStatementToQueue(tblFieldsStringUpdateStatement);
        sqlWriteManager.addStatementToQueue(tblFieldsIntUpdateStatement);
        sqlWriteManager.addStatementToQueue(tblShardsInsertStatement);
        sqlWriteManager.run();
    }

    private void deleteFromSQL(final List<String> allPaths) {
        Runnable run = () -> dbConnection.batchUpdate("DELETE FROM tblshards WHERE path = ?;", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, allPaths.get(i));
            }

            @Override
            public int getBatchSize() {
                return allPaths.size();
            }
        });
        sqlWriteManager.addStatementToQueue(run);
        sqlWriteManager.run();
    }

    public void run(final boolean leader, final boolean shouldDelete) {
        try {
            executorService.submit(() -> innerRun(leader, shouldDelete)).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        if(!setup.get()) {
            LOGGER.info("Finished setup");
            this.setup.set(true);
        }
    }
}
