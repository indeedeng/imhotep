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
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.StreamSupport;

/**
 * @author kenh
 */

class DatasetShardRefresher extends TimerTask {
    private static final Logger LOGGER = Logger.getLogger(DatasetShardRefresher.class);
    private final Path datasetsDir;
    private final HostsReloader hostsReloader;
    private final ShardAssigner shardAssigner;
    private final ShardAssignmentInfoDao assignmentInfoDao;
    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private final String leaderPath;
    private Connection dbConnection;
    private ZooKeeperConnection zkConnection;
    private final org.apache.hadoop.fs.FileSystem hadoopFileSystem;

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
            LOGGER.info("Children are: "+ children);
            LOGGER.info("I am: "+leaderPath.substring(leaderPathEndIndex)+1);
            return children.get(0).equals(leaderPath.substring(leaderPathEndIndex+1));
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e.getCause());
            return false;
        }
    }

    DatasetShardRefresher(final Path datasetsDir,
                          final HostsReloader hostsReloader,
                          final ShardAssigner shardAssigner,
                          final ShardAssignmentInfoDao assignmentInfoDao,
                          final String leaderPath,
                          final ZooKeeperConnection zkConnection,
                          final Connection dbConnection,
                          String rootURI) throws IOException {
        this.datasetsDir = datasetsDir;
        this.hostsReloader = hostsReloader;
        this.shardAssigner = shardAssigner;
        this.assignmentInfoDao = assignmentInfoDao;
        this.leaderPath = leaderPath;
        this.zkConnection = zkConnection;
        this.dbConnection = dbConnection;
        this.hadoopFileSystem = new Path(rootURI).getFileSystem(new Configuration());
    }

    Future initialize() {
        return executorService.submit(this::innerRun);
    }

    private void loadFromSQL() {
        try {
            final PreparedStatement statement = dbConnection.prepareStatement("SELECT * FROM tblshards;");
            final ResultSet set = statement.executeQuery();
            Map<String, List<ShardDir>> shardDirs = ShardData.getInstance().addTableShardsRowsFromSQL(set);
            for (String dataset : shardDirs.keySet()) {
                Iterable<ShardAssignmentInfo> assignment = shardAssigner.assign(hostsReloader.getHosts(), dataset, shardDirs.get(dataset));
                assignmentInfoDao.updateAssignments(dataset, DateTime.now(), assignment);
            }

            final PreparedStatement statement2 = dbConnection.prepareStatement("SELECT * FROM tblfields;");
            if(statement2 == null) {
                LOGGER.error("SQL statement is null. Will not load data from SQL.");
            } else {
                final ResultSet set2 = statement2.executeQuery();
                ShardData.getInstance().addTableFieldsRowsFromSQL(set2);
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
            loadFromSQL();
            DataSetScanner scanner = new DataSetScanner(datasetsDir, hadoopFileSystem);
            StreamSupport.stream(scanner.spliterator(), true).forEach(this::handleDataset);
        } else {
            loadFromSQL();
        }
    }

    // TODO: delete
    private void handleDataset(Path path) {
        Iterable<ShardDir> dataset = new ShardScanner(path, hadoopFileSystem);

        //TODO: hack b/c we are adding stuff so the iterator is empty when we try and assign
        Iterator<ShardDir> datasetIterator = dataset.iterator();

        StreamSupport.stream(dataset.spliterator(), true).forEach(shardDir -> collectShardData(shardDir));
        Iterable<ShardAssignmentInfo> assignment = shardAssigner.assign(hostsReloader.getHosts(), path.getName(), () -> datasetIterator);
        assignmentInfoDao.updateAssignments(path.getName(), DateTime.now(), assignment);
    }

    private void collectShardData(ShardDir shardDir) {
        try {
            FlamdexMetadata metadata = FlamdexMetadata.readMetadata(hadoopFileSystem, shardDir.getHadoopPath());
            if (metadata == null) {
                LOGGER.error("Metadata was null for shard " + shardDir);
            }
            addToSQL(metadata, shardDir);
            ShardData.getInstance().addShardFromHDFS(metadata, shardDir.getIndexDir(), shardDir);
        } catch (IOException | SQLException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void addToSQL(FlamdexMetadata metadata, ShardDir shardDir) throws SQLException {
        String shardId = shardDir.getId();
        String dataset = shardDir.getIndexDir().getParent().toString();
        final PreparedStatement tblShardsInsertStatement = dbConnection.prepareStatement("INSERT INTO tblshards (path, numDocs) VALUES (?, ?);");
        tblShardsInsertStatement.setString(1, shardDir.getIndexDir().toString());
        tblShardsInsertStatement.setInt(2, metadata.getNumDocs());

        ShardData data = ShardData.getInstance();

        final PreparedStatement tblFieldsInsertStatement =
                dbConnection.prepareStatement("INSERT INTO tblfields (dataset, fieldname, type, lastshardstarttime) VALUES (?, ?, ?, ?);");

        metadata.getStringFields().stream().filter(field -> !data.hasField(dataset, field)).forEach(
                field -> {
                    try {
                        tblFieldsInsertStatement.setString(1, dataset);
                        tblFieldsInsertStatement.setString(2, field);
                        tblFieldsInsertStatement.setInt(3, ShardData.fieldType.STRING.getValue());
                        tblFieldsInsertStatement.setLong(4, ShardTimeUtils.parseStart(shardId).getMillis());
                        tblFieldsInsertStatement.addBatch();
                    } catch (SQLException e){
                        e.printStackTrace();
                    }
                }
        );

        metadata.getIntFields().stream().filter(field -> !data.hasField(dataset, field)).forEach(
                field -> {
                    try {
                        tblFieldsInsertStatement.setString(1, dataset);
                        tblFieldsInsertStatement.setString(2, field);
                        tblFieldsInsertStatement.setInt(3, ShardData.fieldType.INT.getValue());
                        tblFieldsInsertStatement.setLong(4, ShardTimeUtils.parseStart(shardId).getMillis());
                        tblFieldsInsertStatement.addBatch();
                    } catch (SQLException e){
                        e.printStackTrace();
                    }
                }
        );

        final PreparedStatement tblFieldsUpdateStatement =
                dbConnection.prepareStatement("UPDATE tblfields SET type = ? AND lastshardstarttime = ? WHERE dataset = ? AND fieldname = ?;");

        metadata.getStringFields().stream().filter(field -> data.hasField(dataset, field) &&
                data.getFieldUpdateTime(dataset, field) < (ShardTimeUtils.parseStart(shardId)).getMillis()).forEach(
                        field -> {
                            try {
                                tblFieldsUpdateStatement.setInt(1, ShardData.fieldType.STRING.getValue());
                                tblFieldsUpdateStatement.setLong(2, ShardTimeUtils.parseStart(shardId).getMillis());
                                tblFieldsUpdateStatement.setString(3, dataset);
                                tblFieldsUpdateStatement.setString(4, field);
                                tblFieldsUpdateStatement.addBatch();
                            } catch (SQLException e){
                                e.printStackTrace();
                            }
                        }
        );

        metadata.getIntFields().stream().filter(field -> data.hasField(dataset, field) &&
                data.getFieldUpdateTime(dataset, field) < (ShardTimeUtils.parseStart(shardId)).getMillis()).forEach(
                field -> {
                    try {
                        tblFieldsUpdateStatement.setInt(1, ShardData.fieldType.INT.getValue());
                        tblFieldsUpdateStatement.setLong(2, ShardTimeUtils.parseStart(shardId).getMillis());
                        tblFieldsUpdateStatement.setString(3, dataset);
                        tblFieldsUpdateStatement.setString(4, field);
                        tblFieldsUpdateStatement.addBatch();
                    } catch (SQLException e){
                        e.printStackTrace();
                    }
                }
        );

        /*
         * In this order because it is more resilient to a crash (don't want to think
         * that a shard has been uploaded when it has not been)
         */
        tblFieldsInsertStatement.executeBatch();
        tblFieldsUpdateStatement.executeBatch();

        tblShardsInsertStatement.execute();
    }

    @Override
    public void run() {
        executorService.submit(this::innerRun);
    }
}
