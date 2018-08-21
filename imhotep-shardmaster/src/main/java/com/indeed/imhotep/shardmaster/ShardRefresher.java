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
import com.indeed.imhotep.hadoopcommon.HDFSUtils;
import com.indeed.imhotep.shardmaster.utils.SQLWriteManager;
import com.indeed.util.core.threads.NamedThreadFactory;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author kornerup
 */

public class ShardRefresher {
    private static final Logger LOGGER = Logger.getLogger(ShardRefresher.class);
    private static final ThreadPoolExecutor datasetsExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(10,
            new NamedThreadFactory("DatasetRefresher"));
    private static final ThreadPoolExecutor shardsExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(100,
            new NamedThreadFactory("ShardRefresher"));
    private final Path datasetsDir;
    private final JdbcTemplate dbConnection;
    private final org.apache.hadoop.fs.FileSystem hadoopFileSystem;
    private final ShardFilter filter;
    private final ShardData shardData;
    private Timestamp lastUpdatedTimestamp;
    private final SQLWriteManager sqlWriteManager;
    private final AtomicInteger numDatasetsReadFromFilesystemOnCurrentRefresh;



    ShardRefresher(final Path datasetsDir,
                   final JdbcTemplate dbConnection,
                   final ShardFilter filter,
                   final ShardData shardData,
                   final SQLWriteManager manager) throws IOException {
        this.datasetsDir = datasetsDir;
        this.dbConnection = dbConnection;
        final Configuration hdfsConfiguration = HDFSUtils.getOurHDFSConfiguration();
        this.hadoopFileSystem = new Path(datasetsDir.toString()).getFileSystem(hdfsConfiguration);
        this.filter = filter;
        this.shardData = shardData;
        this.sqlWriteManager = manager;
        this.lastUpdatedTimestamp = Timestamp.from(Instant.MIN);
        this.numDatasetsReadFromFilesystemOnCurrentRefresh = new AtomicInteger();
    }

    public void refresh(final boolean readFilesystem, final boolean readSQL, final boolean delete, final boolean writeSQL) {
        numDatasetsReadFromFilesystemOnCurrentRefresh.set(0);
        LOGGER.info("Starting a refresh. ReadFilesystem: " + readFilesystem + " readSQL: " + readSQL + " delete: " + delete + "writeSQL: " + writeSQL);
        ScheduledExecutorService updates = Executors.newSingleThreadScheduledExecutor();
        final long startTime = System.currentTimeMillis();
        updates.scheduleAtFixedRate(() -> LOGGER.info("I have a total of: " + shardData.getAllPaths().size() + " shards read. " +
                "There are a total of: " + shardsExecutorService.getActiveCount()  + " shard threads and " +
                datasetsExecutorService.getActiveCount() + " dataset threads active. " +
                "I have used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + " bytes of memory. " +
                "This task has been running for: " + (System.currentTimeMillis() - startTime)/60000 + " minutes. I have finished reading " + numDatasetsReadFromFilesystemOnCurrentRefresh.get() + " datasets from the file system."), 0, 1, TimeUnit.MINUTES);
        long time = -System.currentTimeMillis();
        innerRun(readFilesystem, readSQL, delete,  writeSQL);
        time += System.currentTimeMillis();
        LOGGER.info("Finished a refresh in: " + time + " millis");
        updates.shutdownNow();
    }

    private void innerRun(final boolean readFilesystem, final boolean readSQL, final boolean delete, final boolean writeSQL) {
        if(readSQL) {
            loadFromSQL(delete);
        }
        if(readFilesystem) {
            try {
                scanFilesystemAndUpdateData(writeSQL, delete);
            } catch (IOException e) {
                LOGGER.error("Error reading datasets", e);
            }
        }
    }

    private void loadFromSQL(boolean shouldDelete) {
        final Timestamp timestampToUse = lastUpdatedTimestamp;
        lastUpdatedTimestamp = Timestamp.from(Instant.now());
        if(shouldDelete){
            dbConnection.query("SELECT * FROM tblshards;", (ResultSetExtractor<Void>) rs -> {
                shardData.updateTableShardsRowsFromSQL(rs, true, filter);
                return null;
            });
        } else {
            dbConnection.query("SELECT * FROM tblshards WHERE addedtimestamp >= ?;", statement -> statement.setTimestamp(1, timestampToUse), (ResultSetExtractor<Void>) rs -> {
                shardData.updateTableShardsRowsFromSQL(rs, false, filter);
                return null;
            });
        }

        dbConnection.query("SELECT * FROM tblfields;", (ResultSetExtractor<ResultSet>) rs -> {
            shardData.updateTableFieldsRowsFromSQL(rs, filter);
            return null;
        });
    }

    private void scanFilesystemAndUpdateData(final boolean writeToSQL, final boolean delete) throws IOException {
        final List<Pair<Path, Future>> futures = new ArrayList<>();
        final List<Path> datasets = getDatasets();
        final Set<String> shardsInDatastructureThatMightBeDeleted;
        if(delete) {
            shardsInDatastructureThatMightBeDeleted = shardData.getCopyOfAllPaths();
        } else {
            shardsInDatastructureThatMightBeDeleted = Collections.emptySet();
        }

        for (final Path dataset: datasets) {
            if (filter.accept(dataset.getName())) {
                futures.add(new Pair<>(dataset, datasetsExecutorService.submit(() -> {
                    scanShardsInFilesystem(dataset, writeToSQL, shardsInDatastructureThatMightBeDeleted);
                    numDatasetsReadFromFilesystemOnCurrentRefresh.getAndIncrement();
                })));
            }
        }

        for(final Pair<Path, Future> pair: futures) {
            try {
                pair.getValue().get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Error reading dataset: " + pair.getKey(), e);
            }
        }

        if(delete) {
            shardData.deleteShards(shardsInDatastructureThatMightBeDeleted);
            final List<String> deletedDatasets = shardData.deleteDatasetsWithoutShards();

            if(writeToSQL) {
                deleteShardsInSQL(new ArrayList<>(shardsInDatastructureThatMightBeDeleted));
                deleteFieldsForDatasetsInSQL(deletedDatasets);
            }
        }
    }
    //TODO: check order of deletes

    private List<Path> getDatasets() throws IOException {
        FileStatus[] fStatus = hadoopFileSystem.listStatus(datasetsDir);
        return Arrays.stream(fStatus).filter(FileStatus::isDirectory).map(FileStatus::getPath).collect(Collectors.toList());
    }

    private void scanShardsInFilesystem(final Path datasetPath, final boolean writeToSQL, final Set<String> allExistingPaths) {
        final List<ShardDir> shardDirs = getAllShardsForDatasetInReverseOrder(datasetPath);
        if(allExistingPaths.size()>0) {
            allExistingPaths.removeAll(shardDirs.stream().map(ShardDir::getIndexDir).map(java.nio.file.Path::toString).collect(Collectors.toList()));
        }

        final List<Pair<ShardDir, Future<FlamdexMetadata>>> pairs = getMetadataFutures(shardDirs.stream().filter(this::isValidAndNew).collect(Collectors.toList()));

        for(Pair<ShardDir, Future<FlamdexMetadata>> shardDirMetadataPair: pairs) {
            try {
                final ShardDir shardDir = shardDirMetadataPair.getKey();
                final FlamdexMetadata metadata = shardDirMetadataPair.getValue().get();

                if(metadata == null) {
                    continue;
                }

                if(writeToSQL) {
                    addToSQL(shardDir, metadata);
                }
                shardData.addShardFromFilesystem(shardDir, metadata);
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Could not get metadata for shard", e);
            }

        }

    }

    private List<ShardDir> getAllShardsForDatasetInReverseOrder(Path datasetPath) {
        List<ShardDir> shardDirs;
        try {
            final FileStatus[] fileStatuses = hadoopFileSystem.listStatus(datasetPath);
            shardDirs =  Arrays.stream(fileStatuses).map(file -> new ShardDir(file.getPath())).collect(Collectors.toList());
        } catch (IOException e) {
            LOGGER.error("Could not read shards in dataset: " + datasetPath + " returning no shards for this dataset.", e);
            shardDirs = new ArrayList<>();
        }

        shardDirs.sort((a, b) -> b.getId().compareTo(a.getId()));
        return shardDirs;
    }

    private boolean isValidAndNew(final ShardDir temp) {
        final String dataset = temp.getDataset();
        final String id = temp.getId();
        return !shardData.hasShard(dataset + "/" + temp.getName()) && ShardTimeUtils.isValidShardId(id);
    }

    private List<Pair<ShardDir, Future<FlamdexMetadata>>> getMetadataFutures(Iterable<ShardDir> shardScanner) {
        final List<Pair<ShardDir, Future<FlamdexMetadata>>> pairs = new ArrayList<>();

        for(final ShardDir dir: shardScanner) {
            pairs.add(new Pair<>(dir, shardsExecutorService.submit(() -> {
                try {
                    return collectShardMetadata(dir);
                } catch (IOException e) {
                    LOGGER.error("Error reading metadata for shard: " + dir + ". Skipping this shard.", e);
                    return null;
                }
            })));
        }
        return pairs;
    }

    private FlamdexMetadata collectShardMetadata(final ShardDir shardDir) throws IOException {
        return FlamdexMetadata.readMetadata(hadoopFileSystem, shardDir.getHadoopPath());
    }

    private void addToSQL(final ShardDir shardDir, final FlamdexMetadata metadata)  {
        final String shardId = shardDir.getId();
        final String dataset = shardDir.getDataset();
        Runnable tblShardsInsertStatement = () -> dbConnection.update("INSERT INTO tblshards (path, numDocs) VALUES (?, ?);", statement -> {
            statement.setString(1, shardDir.getIndexDir().toString());
            statement.setInt(2, metadata.getNumDocs());
        });

        final long startTime = ShardTimeUtils.parseStart(shardId).getMillis();

        Set<String> stringFields = new HashSet<>(metadata.getStringFields());
        Set<String> intFields = new HashSet<>(metadata.getIntFields());


        final Stream<String> stringFieldsForInsert = metadata.getStringFields().stream().filter(field -> (!shardData.hasField(dataset, field)));
        final Stream<String> intFieldsForInsert = metadata.getIntFields().stream().filter(field -> (!shardData.hasField(dataset, field)));
        final Stream<String> stringFieldsForUpdate = metadata.getStringFields().stream().filter(field -> shardData.hasField(dataset, field) && shardData.getFieldUpdateTime(dataset, field) < startTime);
        final Stream<String> intFieldsForUpdate = metadata.getIntFields().stream().filter(field -> shardData.hasField(dataset, field) && shardData.getFieldUpdateTime(dataset, field) < startTime);

        final List<String> fieldsForInsert = Stream.concat(stringFieldsForInsert, intFieldsForInsert).collect(Collectors.toList());
        final List<String> fieldsForUpdate = Stream.concat(stringFieldsForUpdate, intFieldsForUpdate).collect(Collectors.toList());


        Runnable tblFieldsInsertStatement = () -> dbConnection.batchUpdate("INSERT INTO tblfields (dataset, fieldname, type, lastshardstarttime) VALUES (?, ?, ?, ?);", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                final String fieldName = fieldsForInsert.get(i);
                ps.setString(1, dataset);
                ps.setString(2, fieldName);

                if(!stringFields.contains(fieldName) && intFields.contains(fieldName)) {
                    ps.setString(3, "INT");
                } else if (stringFields.contains(fieldName) && !intFields.contains(fieldName)) {
                    ps.setString(3, "STRING");
                } else {
                    ps.setString(3, "CONFLICT");
                }

                ps.setLong(4, startTime);
            }

            @Override
            public int getBatchSize() {
                return fieldsForInsert.size();
            }
        });

        Runnable tblFieldsUpdateStatement = () -> dbConnection.batchUpdate("UPDATE tblfields SET type = ?, lastshardstarttime = ? WHERE dataset = ? AND fieldname = ?;", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                final String fieldName = fieldsForUpdate.get(i);

                if(!stringFields.contains(fieldName) && intFields.contains(fieldName)) {
                    ps.setString(1, "INT");
                } else if (stringFields.contains(fieldName) && !intFields.contains(fieldName)) {
                    ps.setString(1, "STRING");
                } else {
                    ps.setString(1, "CONFLICT");
                }

                ps.setLong(2, startTime);
                ps.setString(3, dataset);
                ps.setString(4, fieldName);
            }

            @Override
            public int getBatchSize() {
                return fieldsForUpdate.size();
            }
        });

        if(fieldsForInsert.size() > 0) {
            sqlWriteManager.addStatementToQueue(tblFieldsInsertStatement);
        }

        if(fieldsForUpdate.size() > 0) {
            sqlWriteManager.addStatementToQueue(tblFieldsUpdateStatement);
        }

        sqlWriteManager.addStatementToQueue(tblShardsInsertStatement);
        sqlWriteManager.run();
    }

    private void deleteFieldsForDatasetsInSQL(List<String> deletedDatasets) {
        Runnable deleteStatement = () -> dbConnection.batchUpdate("DELETE FROM tblfields WHERE dataset = ?;", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, deletedDatasets.get(i));
            }

            @Override
            public int getBatchSize() {
                return deletedDatasets.size();
            }
        });

        sqlWriteManager.addStatementToQueue(deleteStatement);
        sqlWriteManager.run();
    }

    private void deleteShardsInSQL(final List<String> allPaths) {
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
}
