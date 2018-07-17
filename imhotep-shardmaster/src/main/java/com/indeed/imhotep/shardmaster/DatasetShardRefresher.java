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
import com.indeed.util.zookeeper.ZooKeeperConnection;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.StreamSupport;

/**
 * @author kenh
 */

class DatasetShardRefresher extends TimerTask {
    private static final Logger LOGGER = Logger.getLogger(DatasetShardRefresher.class);
    private final Path datasetsDir;
    private static final ExecutorService executorService = new ForkJoinPool(40);
    private final String leaderPath;
    private Connection dbConnection;
    private ZooKeeperConnection zkConnection;
    private final org.apache.hadoop.fs.FileSystem hadoopFileSystem;
    private final ShardFilter filter;
    private final ShardData shardData;

    public boolean isLeader(){
        try {
            int leaderPathEndIndex = leaderPath.lastIndexOf('/');
            if(leaderPathEndIndex == -1) {
                LOGGER.error("The leader path is corrupted. Assuming that I am the leader.");
                return true;
            }

            final String electionRoot = leaderPath.substring(0, leaderPathEndIndex);
            List<String> children = zkConnection.getChildren(electionRoot, false);
            Collections.sort(children);
            return children.get(0).equals(leaderPath.substring(leaderPathEndIndex+1));
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e.getCause());
            return true;
        }
    }

    DatasetShardRefresher(final Path datasetsDir,
                          final String leaderPath,
                          final ZooKeeperConnection zkConnection,
                          final Connection dbConnection,
                          final String rootURI,
                          final ShardFilter filter,
                          ShardData shardData) throws IOException {
        this.datasetsDir = datasetsDir;
        this.leaderPath = leaderPath;
        this.zkConnection = zkConnection;
        this.dbConnection = dbConnection;
        this.hadoopFileSystem = new Path(rootURI).getFileSystem(new Configuration());
        this.filter = filter;
        this.shardData = shardData;
    }

    Future initialize() {
        return executorService.submit(this::innerRun);
    }

    private void loadFromSQL() {
        try {
            final PreparedStatement statement = dbConnection.prepareStatement("SELECT * FROM tblshards;");
            final ResultSet set = statement.executeQuery();
            shardData.addTableShardsRowsFromSQL(set);

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
    private synchronized void innerRun() {
        if(isLeader()) {
            LOGGER.info("I am leader!");
            loadFromSQL();
            DataSetScanner scanner = new DataSetScanner(datasetsDir, hadoopFileSystem);
            try {
                executorService.submit(() -> StreamSupport.stream(scanner.spliterator(), true).filter(path -> filter.accept(path.getName())).forEach(this::handleDataset)).get();
            } catch (ExecutionException | InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        } else {
            loadFromSQL();
        }
        LOGGER.info("Finished innerRun");
    }

    private void handleDataset(Path path) {
        LOGGER.info("On dataset: " + path);
        Iterable<ShardDir> dataset = new ShardScanner(path, hadoopFileSystem);

        //TODO: hack b/c we are adding stuff so the iterator is empty when we try and assign
        Iterator<ShardDir> datasetIterator = dataset.iterator();

        List<Pair<ShardDir, Future<FlamdexMetadata>>> pairs = new ArrayList<>();

        for(final ShardDir dir: dataset) {
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

    private boolean isValidAndNew(ShardDir temp) {
        String dataset = temp.getDataset();
        String id = temp.getId();
        return !shardData.hasShard(dataset + "/" + temp.getName()) && ShardTimeUtils.isValidShardId(id);
    }

    private FlamdexMetadata collectShardMetadata(ShardDir shardDir) throws IOException {
        LOGGER.info("reading a shard with dataset: " + shardDir.getDataset() + " and id: " + shardDir.getId() + " and version " + shardDir.getVersion());
        FlamdexMetadata metadata = FlamdexMetadata.readMetadata(hadoopFileSystem, shardDir.getHadoopPath());
        LOGGER.info("finished reading a shard with dataset: " + shardDir.getDataset() + " and id: " + shardDir.getId() + " and version " + shardDir.getVersion());
        return metadata;
    }

    private void addData(ShardDir shardDir, FlamdexMetadata metadata) throws SQLException {
        addToSQL(metadata, shardDir);
        shardData.addShardFromHDFS(metadata, shardDir.getIndexDir(), shardDir);
    }

    private void addToSQL(FlamdexMetadata metadata, ShardDir shardDir) throws SQLException {
        final String shardId = shardDir.getId();
        final String dataset = shardDir.getDataset();
        final PreparedStatement tblShardsInsertStatement = dbConnection.prepareStatement("INSERT INTO tblshards (path, numDocs) VALUES (?, ?);");
        tblShardsInsertStatement.setString(1, shardDir.getIndexDir().toString());
        tblShardsInsertStatement.setInt(2, metadata.getNumDocs());

        final PreparedStatement tblFieldsInsertStatement =
                dbConnection.prepareStatement("INSERT INTO tblfields (dataset, fieldname, type, lastshardstarttime) VALUES (?, ?, ?, ?);");

        final long startTime = ShardTimeUtils.parseStart(shardId).getMillis();

        for (String field : metadata.getStringFields()) {
            if (!shardData.hasField(dataset, field)) {
                try {
                    tblFieldsInsertStatement.setString(1, dataset);
                    tblFieldsInsertStatement.setString(2, field);
                    tblFieldsInsertStatement.setInt(3, ShardData.FieldType.STRING.getValue());
                    tblFieldsInsertStatement.setLong(4, startTime);
                    tblFieldsInsertStatement.addBatch();
                } catch (SQLException e) {
                    LOGGER.error("Failed to insert string fields", e);
                }
            }
        }

        for (String field : metadata.getIntFields()) {
            if (!shardData.hasField(dataset, field)) {
                try {
                    tblFieldsInsertStatement.setString(1, dataset);
                    tblFieldsInsertStatement.setString(2, field);
                    tblFieldsInsertStatement.setInt(3, ShardData.FieldType.INT.getValue());
                    tblFieldsInsertStatement.setLong(4, startTime);
                    tblFieldsInsertStatement.addBatch();
                } catch (SQLException e) {
                    LOGGER.error("Failed to insert int fields", e);
                }
            }
        }

        final PreparedStatement tblFieldsUpdateStatement =
                dbConnection.prepareStatement("UPDATE tblfields SET type = ?, lastshardstarttime = ? WHERE dataset = ? AND fieldname = ?;");

        for (String field : metadata.getStringFields()) {
            if (shardData.hasField(dataset, field) &&
                    shardData.getFieldUpdateTime(dataset, field) < startTime) {
                try {
                    tblFieldsUpdateStatement.setInt(1, ShardData.FieldType.STRING.getValue());
                    tblFieldsUpdateStatement.setLong(2, startTime);
                    tblFieldsUpdateStatement.setString(3, dataset);
                    tblFieldsUpdateStatement.setString(4, field);
                    tblFieldsUpdateStatement.addBatch();
                } catch (SQLException e) {
                    LOGGER.error("Failed to update string fields", e);
                }
            }
        }

        for (String field : metadata.getIntFields()) {
            if (shardData.hasField(dataset, field) &&
                    shardData.getFieldUpdateTime(dataset, field) < startTime) {
                try {
                    tblFieldsUpdateStatement.setInt(1, ShardData.FieldType.INT.getValue());
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

    @Override
    public void run() {
        executorService.submit(this::innerRun);
    }
}
