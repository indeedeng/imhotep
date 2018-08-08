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
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.StreamSupport;


/**
 * @author kenh
 */

class DatasetShardRefresher {
    private static final Logger LOGGER = Logger.getLogger(DatasetShardRefresher.class);
    private final Path datasetsDir;
    private static final ExecutorService executorService = new ForkJoinPool(40);
    private final Connection dbConnection;
    private final org.apache.hadoop.fs.FileSystem hadoopFileSystem;
    private final ShardFilter filter;
    private final ShardData shardData;
    private long lastUpdatedTimestamp = 0;


    DatasetShardRefresher(final Path datasetsDir,
                          final Connection dbConnection,
                          final String rootURI,
                          final ShardFilter filter,
                          ShardData shardData) throws IOException {
        this.datasetsDir = datasetsDir;
        this.dbConnection = dbConnection;
        this.hadoopFileSystem = new Path(rootURI).getFileSystem(new Configuration());
        this.filter = filter;
        this.shardData = shardData;
    }

    Future initialize(final boolean leader) {
        return executorService.submit(() -> innerRun(leader));
    }

    private void loadFromSQL() {
        try {
            final long timestampToUse = lastUpdatedTimestamp;
            lastUpdatedTimestamp = System.currentTimeMillis();
            final PreparedStatement statement = dbConnection.prepareStatement("SELECT * FROM tblshards WHERE addedtimestamp >= ?;");
            statement.setLong(1, timestampToUse);
            final ResultSet set = statement.executeQuery();
            shardData.updateTableShardsRowsFromSQL(set);

            final PreparedStatement statement2 = dbConnection.prepareStatement("SELECT * FROM tblfields;");
            if(statement2 == null) {
                LOGGER.error("SQL statement is null. Will not load data from SQL.");
            } else {
                final ResultSet set2 = statement2.executeQuery();
                shardData.addTableFieldsRowsFromSQL(set2);
            }
        } catch (SQLException e){
            LOGGER.error(e.getMessage(), e);
        }

    }

    /*
     * Synchronized so if something switches to being a leader while a lot of shards still
     * need to be uploaded to SQL, it won't duplicate work.
     */
    private synchronized void innerRun(boolean leader) {
        loadFromSQL();

        if(!leader) {
            return;
        }

        DataSetScanner scanner = new DataSetScanner(datasetsDir, hadoopFileSystem);
        try {
            final Set<String> allRemaining = shardData.getCopyOfAllPaths();
            executorService.submit(() -> StreamSupport.stream(scanner.spliterator(), true).filter(path -> filter.accept(path.getName())).forEach(a -> handleDataset(a, allRemaining))).get();

            deleteFromSQL(allRemaining);
            shardData.deleteShards(allRemaining);
        } catch (ExecutionException | InterruptedException | SQLException e) {
            LOGGER.error(e.getMessage(), e);
        }
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
        final PreparedStatement tblShardsInsertStatement = dbConnection.prepareStatement("INSERT INTO tblshards (path, numDocs, addedtimestamp) VALUES (?, ?, ?);");
        tblShardsInsertStatement.setString(1, shardDir.getIndexDir().toString());
        tblShardsInsertStatement.setInt(2, metadata.getNumDocs());
        tblShardsInsertStatement.setLong(3, System.currentTimeMillis());

        final PreparedStatement tblFieldsInsertStatement =
                dbConnection.prepareStatement("INSERT INTO tblfields (dataset, fieldname, type, lastshardstarttime) VALUES (?, ?, ?, ?);");

        final long startTime = ShardTimeUtils.parseStart(shardId).getMillis();

        for (final String field : metadata.getStringFields()) {
            if (!shardData.hasField(dataset, field)) {
                try {
                    tblFieldsInsertStatement.setString(1, dataset);
                    tblFieldsInsertStatement.setString(2, field);
                    tblFieldsInsertStatement.setString(3, "STRING");
                    tblFieldsInsertStatement.setLong(4, startTime);
                    tblFieldsInsertStatement.addBatch();
                } catch (SQLException e) {
                    LOGGER.error("Failed to insert string fields", e);
                }
            }
        }

        for (final String field : metadata.getIntFields()) {
            if (!shardData.hasField(dataset, field)) {
                try {
                    tblFieldsInsertStatement.setString(1, dataset);
                    tblFieldsInsertStatement.setString(2, field);
                    tblFieldsInsertStatement.setString(3, "INT");
                    tblFieldsInsertStatement.setLong(4, startTime);
                    tblFieldsInsertStatement.addBatch();
                } catch (SQLException e) {
                    LOGGER.error("Failed to insert int fields", e);
                }
            }
        }

        final PreparedStatement tblFieldsUpdateStatement =
                dbConnection.prepareStatement("UPDATE tblfields SET type = ?, lastshardstarttime = ? WHERE dataset = ? AND fieldname = ?;");

        for (final String field : metadata.getStringFields()) {
            if (shardData.hasField(dataset, field) &&
                    shardData.getFieldUpdateTime(dataset, field) < startTime) {
                try {
                    tblFieldsUpdateStatement.setString(1, "STRING");
                    tblFieldsUpdateStatement.setLong(2, startTime);
                    tblFieldsUpdateStatement.setString(3, dataset);
                    tblFieldsUpdateStatement.setString(4, field);
                    tblFieldsUpdateStatement.addBatch();
                } catch (SQLException e) {
                    LOGGER.error("Failed to update string fields", e);
                }
            }
        }

        for (final String field : metadata.getIntFields()) {
            if (shardData.hasField(dataset, field) &&
                    shardData.getFieldUpdateTime(dataset, field) < startTime) {
                try {
                    tblFieldsUpdateStatement.setString(1, "INT");
                    tblFieldsUpdateStatement.setLong(2, startTime);
                    tblFieldsUpdateStatement.setString(3, dataset);
                    tblFieldsUpdateStatement.setString(4, field);
                    tblFieldsUpdateStatement.addBatch();
                } catch (SQLException e) {
                    LOGGER.error("Failed to update int fields", e);
                }
            }
        }

        tblFieldsInsertStatement.executeBatch();
        tblFieldsUpdateStatement.executeBatch();

        tblShardsInsertStatement.execute();
    }

    private void deleteFromSQL(final Set<String> allPaths) throws SQLException {
        final PreparedStatement tblShardsDeleteStatement = dbConnection.prepareStatement("DELETE FROM tblshards WHERE path = ?;");
        for(final String path: allPaths) {
            tblShardsDeleteStatement.setString(1, path);
            tblShardsDeleteStatement.addBatch();
        }
        tblShardsDeleteStatement.executeBatch();

    }

    public void run(final boolean leader) {
        executorService.submit(() -> innerRun(leader));
    }
}
