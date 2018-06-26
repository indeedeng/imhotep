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
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.StreamSupport;

/**
 * @author kenh
 */

class DatasetShardRefresher extends TimerTask {
    private static final Logger LOGGER = Logger.getLogger(DatasetShardRefresher.class);
    private final RemoteCachingPath datasetsDir;
    private final HostsReloader hostsReloader;
    private final ShardAssigner shardAssigner;
    private final ShardAssignmentInfoDao assignmentInfoDao;
    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private final String leaderPath;
    private Future load;
    private Connection dbConnection;
    private ZooKeeperConnection zkConnection;

    public boolean isLeader(){
        try {
            final String electionRoot = leaderPath.substring(0, leaderPath.lastIndexOf('/'));
            List<String> children = zkConnection.getChildren(electionRoot, false);
            Collections.sort(children);
            LOGGER.info("Children are: "+ children);
            LOGGER.info("I am: "+leaderPath.substring(leaderPath.lastIndexOf('/')+1));
            return children.get(0).equals(leaderPath.substring(leaderPath.lastIndexOf('/')+1));
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e.getCause());
            return false;
        }
    }

    DatasetShardRefresher(final RemoteCachingPath datasetsDir,
                          final HostsReloader hostsReloader,
                          final ShardAssigner shardAssigner,
                          final ShardAssignmentInfoDao assignmentInfoDao,
                          final String leaderPath,
                          final ZooKeeperConnection zkConnection,
                          final Connection dbConnection) {
        this.datasetsDir = datasetsDir;
        this.hostsReloader = hostsReloader;
        this.shardAssigner = shardAssigner;
        this.assignmentInfoDao = assignmentInfoDao;
        this.leaderPath = leaderPath;
        this.zkConnection = zkConnection;
        this.dbConnection = dbConnection;
    }

    Future initialize() {
        load = executorService.submit(this::loadFromSQL);
        return load;
    }

    private void loadFromSQL() {
        try {
            ResultSet set = dbConnection.prepareStatement("SELECT * FROM tblshards;").executeQuery();
            Map<String, List<ShardDir>> shardDirs = ShardData.getInstance().addTableShardsRowsFromSQL(set);
            ResultSet set2 = dbConnection.prepareStatement("SELECT * FROM tblfields;").executeQuery();
            ShardData.getInstance().addTableFieldsRowsFromSQL(set2);
            for(String dataset: shardDirs.keySet()) {
                Iterable<ShardAssignmentInfo> assignment = shardAssigner.assign(hostsReloader.getHosts(), dataset, shardDirs.get(dataset));
                assignmentInfoDao.updateAssignments(dataset, DateTime.now(), assignment);
            }
        } catch (SQLException e){
            e.printStackTrace();
        }

    }

    private void innerRun() {
        if(isLeader()) {
            LOGGER.info("I am the leader!");
            try {
                load.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            DataSetScanner scanner = new DataSetScanner(datasetsDir);
            StreamSupport.stream(scanner.spliterator(), true).forEach(this::handleDataset);
        } else {
            LOGGER.info("I am not the leader!");
            loadFromSQL();
        }
    }

    private void handleDataset(RemoteCachingPath path) {
        Iterable<ShardDir> dataset = new ShardScanner(path);

        //TODO: hack b/c we are adding stuff so the iterator is empty when we try and assign
        Iterator<ShardDir> datasetIterator = dataset.iterator();

        StreamSupport.stream(dataset.spliterator(), true).forEach(shardDir -> collectShardData(path, shardDir));
        Iterable<ShardAssignmentInfo> assignment = shardAssigner.assign(hostsReloader.getHosts(), path.getFileName().toString(), () -> datasetIterator);
        assignmentInfoDao.updateAssignments(path.getFileName().toString(), DateTime.now(), assignment);
    }

    private void collectShardData(Path path, ShardDir shardDir) {
        try {
            FlamdexMetadata metadata = FlamdexMetadata.readMetadata(shardDir.getIndexDir());
            addToSQL(metadata, path.getFileName().toString(), shardDir.getId());
            ShardData.getInstance().addShardFromHDFS(metadata, shardDir.getIndexDir(), path.getFileName().toString(), shardDir.getId());
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            //TODO: something
        }
    }

    private void addToSQL(FlamdexMetadata metadata, String dataset, String shardId) throws SQLException {

        System.out.println("adding dataset: " + dataset + " id: " + shardId);
        final PreparedStatement tblShardsInsertStatement = dbConnection.prepareStatement("INSERT INTO tblshards (dataset, shardname, numDocs) VALUES (?, ?, ?);");
        tblShardsInsertStatement.setString(1, dataset);
        tblShardsInsertStatement.setString( 2, shardId);
        tblShardsInsertStatement.setInt(3, metadata.getNumDocs());
        tblShardsInsertStatement.execute();

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

        tblFieldsInsertStatement.executeBatch();

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

        tblFieldsUpdateStatement.executeBatch();
    }

    @Override
    public void run() {
        executorService.submit(this::innerRun);
    }
}
