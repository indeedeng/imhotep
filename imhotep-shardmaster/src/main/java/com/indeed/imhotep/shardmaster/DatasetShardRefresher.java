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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.StreamSupport;

import static com.indeed.imhotep.shardmaster.DatasetShardRefresher.LeaderState.*;

/**
 * @author kenh
 */

class DatasetShardRefresher extends TimerTask {
    private static final Logger LOGGER = Logger.getLogger(DatasetShardRefresher.class);
    private final Path datasetsDir;
    private static final ExecutorService executorService = new ForkJoinPool(40);
    private final String leaderElectionRoot;
    private final String hostId;
    private String leaderId;
    private final Connection dbConnection;
    private final ZooKeeperConnection zkConnection;
    private final org.apache.hadoop.fs.FileSystem hadoopFileSystem;
    private final ShardFilter filter;
    private String lastSelectedLeaderId;

    private final ShardData shardData;

    enum LeaderState {
        NEW_LEADER,
        OLD_LEADER,
        NOT_LEADER,
        ERROR
    }

    public LeaderState isLeader(){
        try {
            if(!zkConnection.isConnected()) {
                zkConnection.connect();
                final String leaderPath = zkConnection.create(leaderElectionRoot+"/_", hostId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                leaderId = leaderPath.substring(leaderElectionRoot.length()+1);
            } else if(leaderId == null) {
                final String leaderPath = zkConnection.create(leaderElectionRoot+"/_", hostId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                leaderId = leaderPath.substring(leaderElectionRoot.length()+1);
            }

            final List<String> children = zkConnection.getChildren(leaderElectionRoot, false);
            Collections.sort(children);

            int index = Collections.binarySearch(children, leaderId);

            if(index == 0) {
                if(leaderId.equals(lastSelectedLeaderId)) {
                    return OLD_LEADER;
                }
                lastSelectedLeaderId = leaderId;
                return NEW_LEADER;
            }

            if(index == -1) {
                LOGGER.info("Lost my leader path. Resetting my connection to zookeeper.");
                zkConnection.delete(leaderElectionRoot + leaderId, -1);
                zkConnection.close();
            }

            return NOT_LEADER;

        } catch (InterruptedException | KeeperException | IOException e ) {
            LOGGER.error(e.getMessage(), e.getCause());
            return ERROR;
        }
    }

    DatasetShardRefresher(final Path datasetsDir,
                          final String leaderElectionRoot,
                          final String hostId,
                          final ZooKeeperConnection zkConnection,
                          final Connection dbConnection,
                          final String rootURI,
                          final ShardFilter filter,
                          ShardData shardData) throws IOException {
        this.datasetsDir = datasetsDir;
        this.zkConnection = zkConnection;
        this.dbConnection = dbConnection;
        this.hadoopFileSystem = new Path(rootURI).getFileSystem(new Configuration());
        this.filter = filter;
        this.shardData = shardData;
        this.leaderElectionRoot = leaderElectionRoot;
        this.hostId = hostId;
    }

    Future initialize() {
        return executorService.submit(this::innerRun);
    }

    private void loadFromSQL() {
        try {
            final PreparedStatement statement = dbConnection.prepareStatement("SELECT * FROM tblshards;");
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
    private synchronized void innerRun() {
        switch (isLeader()) {
            case NEW_LEADER:
                loadFromSQL();
            case OLD_LEADER:
                DataSetScanner scanner = new DataSetScanner(datasetsDir, hadoopFileSystem);
                try {
                    Set<String> allPaths = shardData.getCopyOfAllPaths();
                    executorService.submit(() -> StreamSupport.stream(scanner.spliterator(), true).filter(path -> filter.accept(path.getName())).forEach(a-> handleDataset(a, allPaths))).get();
                    if(allPaths.size() > 0) {
                        LOGGER.info("Deleting shards: " + allPaths + " because they are no longer present on the filesystem.");
                        deleteFromSQL(allPaths);
                        shardData.deleteShards(allPaths);
                    }
                } catch (ExecutionException | InterruptedException | SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                break;
            case NOT_LEADER:
                loadFromSQL();
                break;
            case ERROR:
                LOGGER.error("There was an error with the leader election process. Assuming I am not the leader.");
                loadFromSQL();
                break;
        }
    }


    private void handleDataset(Path path, Set<String> allPaths) {
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

    private void deleteFromSQL(Set<String> allPaths) throws SQLException {
        final PreparedStatement tblShardsInsertStatement = dbConnection.prepareStatement("DELETE FROM tblshards WHERE path = ?;");
        for(String path: allPaths) {
            tblShardsInsertStatement.setString(1, path);
            tblShardsInsertStatement.addBatch();
        }
        tblShardsInsertStatement.executeBatch();

    }

    @Override
    public void run() {
        executorService.submit(this::innerRun);
    }
}
