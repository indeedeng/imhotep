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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import org.joda.time.Period;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final AtomicInteger numDatasetsFailedToRead = new AtomicInteger();
    private final AtomicInteger numDatasetsReadFromFilesystemOnCurrentRefresh = new AtomicInteger();
    private final AtomicInteger numDatasetsCompletedFieldsRefreshOnCurrentRefresh = new AtomicInteger();
    private final AtomicInteger totalDatasetsOnCurrentRefresh = new AtomicInteger();
    private Queue<Runnable> fieldRefreshQueue = new ConcurrentLinkedQueue<>();



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
    }

    public synchronized void refresh(final boolean readFilesystem, final boolean readSQL, final boolean delete, final boolean writeSQL, final boolean shouldRefreshFieldsForDataset) {
        numDatasetsReadFromFilesystemOnCurrentRefresh.set(0);
        numDatasetsCompletedFieldsRefreshOnCurrentRefresh.set(0);
        totalDatasetsOnCurrentRefresh.set(0);
        numDatasetsFailedToRead.set(0);
        LOGGER.info("Starting a refresh. ReadFilesystem: " + readFilesystem + " readSQL: " + readSQL + " delete: " + delete + " writeSQL: " + writeSQL);
        ScheduledExecutorService updates = Executors.newSingleThreadScheduledExecutor();
        final long startTime = System.currentTimeMillis();
        updates.scheduleAtFixedRate(() -> {
                    LOGGER.info("Updated " + numDatasetsReadFromFilesystemOnCurrentRefresh.get() +
                            "/" + totalDatasetsOnCurrentRefresh.get() + " datasets in " + (System.currentTimeMillis() - startTime) / 60000 + " minutes. " +
                            "Known shards: " + shardData.getAllPaths().size() + ". \n" +
                            "Shard update threads: " + shardsExecutorService.getActiveCount() + ". " +
                            "Dataset update threads: " + datasetsExecutorService.getActiveCount() + ". " +
                            "Used heap MB: " + ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024));
                    if(numDatasetsCompletedFieldsRefreshOnCurrentRefresh.get() > 0) {
                        LOGGER.info("Processed complete field refreshes for " + numDatasetsCompletedFieldsRefreshOnCurrentRefresh + " datasets");
                    }
                },
                0, 1, TimeUnit.MINUTES);
        long time = -System.currentTimeMillis();
        // Actually run
        innerRun(readFilesystem, readSQL, delete,  writeSQL, shouldRefreshFieldsForDataset);
        time += System.currentTimeMillis();
        LOGGER.info("Finished a refresh in " + new Period(time));
        if (numDatasetsFailedToRead.get() > 0) {
            LOGGER.error("We have failed to scan " + numDatasetsFailedToRead.get() + " datasets");
        }
        updates.shutdownNow();
    }

    private void innerRun(final boolean readFilesystem, final boolean readSQL, final boolean delete, final boolean writeSQL, final boolean shouldRefreshFieldsForDataset) {
        if (shouldRefreshFieldsForDataset && writeSQL && fieldRefreshQueue.size() > 0) {
            performCompleteFieldsRefreshes();
        } else {
            fieldRefreshQueue = new ArrayDeque<>();
        }
        if(readSQL) {
            long startSQLRead = System.currentTimeMillis();
            loadFromSQL(delete);
            LOGGER.info("Finished update from SQL in " + new Period(System.currentTimeMillis() - startSQLRead));
        }
        if(readFilesystem) {
            try {
                scanFilesystemAndUpdateData(writeSQL, delete);
            } catch (IOException e) {
                LOGGER.error("Error reading datasets", e);
            }
        }
    }

    private void performCompleteFieldsRefreshes() {
        final long startDatasetRefresh = System.currentTimeMillis();
        final int datasetsToRefreshFieldsCount = fieldRefreshQueue.size();
        LOGGER.info("Running " + datasetsToRefreshFieldsCount + " complete dataset field refreshes.");
        List<Future> datasetRefreshFieldsFutures = Lists.newArrayList();
        while (!fieldRefreshQueue.isEmpty()) {
            final Runnable fieldsRefreshRunnable = fieldRefreshQueue.remove();
            datasetRefreshFieldsFutures.add(datasetsExecutorService.submit(() -> {
                fieldsRefreshRunnable.run();
                numDatasetsCompletedFieldsRefreshOnCurrentRefresh.getAndIncrement();
            }));
        }

        for(final Future future: datasetRefreshFieldsFutures) {
            try {
                future.get();
            } catch (Exception e) {
                LOGGER.error("Failure during complete field list refresh for dataset", e);
            }
        }
        LOGGER.info("Finished complete dataset field refreshes in " +  new Period(System.currentTimeMillis() - startDatasetRefresh));
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
        totalDatasetsOnCurrentRefresh.set(datasets.size());
        LOGGER.info("Starting update on " + datasets.size() + " datasets");
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
            if(numDatasetsFailedToRead.get() == 0) {
                if (shardsInDatastructureThatMightBeDeleted.size() > 0) {
                    LOGGER.info("Deleting in memory info for " + shardsInDatastructureThatMightBeDeleted.size() + " deleted shards");
                    shardData.deleteShards(shardsInDatastructureThatMightBeDeleted);
                }

                final List<String> deletedDatasets = shardData.deleteDatasetsWithoutShards();

                if (writeToSQL) {
                    if (shardsInDatastructureThatMightBeDeleted.size() > 0) {
                        LOGGER.info("Deleting SQL rows for " + shardsInDatastructureThatMightBeDeleted.size() + " deleted shards");
                        deleteShardsInSQL(new ArrayList<>(shardsInDatastructureThatMightBeDeleted));
                    }
                    if (deletedDatasets.size() > 0) {
                        LOGGER.info("Deleting SQL rows for all fields in " + deletedDatasets.size() + " deleted datasets");
                        deleteFieldsForDatasetsInSQL(deletedDatasets);
                    }
                }
            } else {
                LOGGER.error("Not performing deletions as " + numDatasetsFailedToRead.get() + " datasets failed to read");
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
            for(ShardDir shardDir : shardDirs) {
                // can't use removeAll() on the KeySetView of ConcurrentHashMap as it's super slow
                allExistingPaths.remove(shardDir.getIndexDir().toString());
            }
        }

        final List<Pair<ShardDir, Future<FlamdexMetadata>>> pairs = getMetadataFutures(shardDirs.stream().filter(this::isValidAndNew).collect(Collectors.toList()));
        long shardsAdded = 0;
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
                shardsAdded++;
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Could not get metadata for shard", e);
            }
        }
        if(shardsAdded > 0) {
            LOGGER.info("Added " + shardsAdded + " shards in dataset " + datasetPath.getName());
        }
    }

    private List<ShardDir> getAllShardsForDatasetInReverseOrder(Path datasetPath) {
        List<ShardDir> shardDirs;
        try {
            final FileStatus[] fileStatuses = hadoopFileSystem.listStatus(datasetPath);
            shardDirs =  Arrays.stream(fileStatuses).map(file -> new ShardDir(file.getPath())).collect(Collectors.toList());
        } catch (IOException e) {
            LOGGER.error("Could not read shards in dataset: " + datasetPath + " returning no shards for this dataset.", e);
            numDatasetsFailedToRead.getAndIncrement();
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

    private void deleteShardsInSQL(final List<String> deleteCandidates) {
        Runnable run = () -> dbConnection.batchUpdate("DELETE FROM tblshards WHERE path = ?;", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, deleteCandidates.get(i));
            }

            @Override
            public int getBatchSize() {
                return deleteCandidates.size();
            }
        });
        sqlWriteManager.addStatementToQueue(run);
        sqlWriteManager.run();
    }

    public void refreshFieldsForDatasetInSQL(final String dataset) throws IOException {
        Runnable refreshTask = () -> {
            try {
                final AllFieldsSet allFieldsForDataset = getAllFieldsForDataset(dataset);
                List<String> allFields = new ArrayList<>(Sets.union(allFieldsForDataset.intFields.keySet(), allFieldsForDataset.strFields.keySet()));
                // TODO: make these calls atomic / a transaction
                Runnable clean = () -> dbConnection.update(con -> {
                    final PreparedStatement statement = con.prepareStatement("DELETE FROM tblfields WHERE dataset = ?");
                    statement.setString(1, dataset);
                    return statement;
                });

                Runnable fill = () -> dbConnection.batchUpdate("INSERT INTO tblfields (dataset, fieldname, type, lastshardstarttime) VALUES (?, ?, ?, ?)", new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        final String fieldName = allFields.get(i);
                        ps.setString(1, dataset);
                        ps.setString(2, fieldName);
                        if (allFieldsForDataset.strFields.containsKey(fieldName) && allFieldsForDataset.intFields.containsKey(fieldName)) {
                            ps.setString(3, "CONFLICT");
                            ps.setLong(4, Math.max(allFieldsForDataset.intFields.get(fieldName), allFieldsForDataset.strFields.get(fieldName)));
                        } else if (allFieldsForDataset.strFields.containsKey(fieldName)) {
                            ps.setString(3, "STRING");
                            ps.setLong(4, allFieldsForDataset.strFields.get(fieldName));
                        } else {
                            ps.setString(3, "INT");
                            ps.setLong(4, allFieldsForDataset.intFields.get(fieldName));
                        }
                    }

                    @Override
                    public int getBatchSize() {
                        return allFields.size();
                    }
                });

                sqlWriteManager.addStatementToQueue(clean);
                sqlWriteManager.addStatementToQueue(fill);
                sqlWriteManager.run();
            } catch (IOException e) {
                LOGGER.error("Could not refresh fields for dataset: " + dataset, e);
            }
        };
        fieldRefreshQueue.add(refreshTask);
    }


    private class AllFieldsSet {
        Map<String, Long> strFields;
        Map<String, Long> intFields;
        public AllFieldsSet() {
            strFields = new HashMap<>();
            intFields = new HashMap<>();
        }
    }

    private AllFieldsSet getAllFieldsForDataset(String dataset) throws IOException {
        final AllFieldsSet toReturn = new AllFieldsSet();
        final Path datasetPath = hadoopFileSystem.resolvePath(new Path(datasetsDir.toString() + "/" + dataset));
        final FileStatus[] fileStatuses = hadoopFileSystem.listStatus(datasetPath);
        List<ShardDir> shardDirs = Arrays.stream(fileStatuses).map(FileStatus::getPath).map(ShardDir::new).collect(Collectors.toList());
        final List<Pair<ShardDir, Future<FlamdexMetadata>>> metadataFutures = getMetadataFutures(shardDirs);
        for(Pair<ShardDir, Future<FlamdexMetadata>> p: metadataFutures) {
            try {
                final FlamdexMetadata metadata = p.getValue().get();
                final long shardStartTime = ShardTimeUtils.parseStart(p.getKey().getId()).getMillis();
                for(String field: metadata.getStringFields()) {
                    if(toReturn.strFields.containsKey(field) && toReturn.strFields.get(field) > shardStartTime) {
                        continue;
                    }
                    toReturn.strFields.put(field, shardStartTime);
                }
                for(String field: metadata.getIntFields()) {
                    if(toReturn.intFields.containsKey(field) && toReturn.intFields.get(field) > shardStartTime) {
                        continue;
                    }
                    toReturn.intFields.put(field, shardStartTime);
                }
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Could not get metadata for shard " + p.getKey().getIndexDir(), e);
            }
        }
        return toReturn;
    }
}
