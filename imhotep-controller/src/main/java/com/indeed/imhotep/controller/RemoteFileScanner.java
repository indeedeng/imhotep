package com.indeed.imhotep.controller;

import com.google.common.collect.ImmutableMap;
import com.indeed.imhotep.fs.SqarManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by darren on 2/25/16.
 */
public class RemoteFileScanner {
    private static final Logger log = Logger.getLogger(RemoteFileScanner.class);
    private static final int DEFAULT_MAX_MEM_USAGE = 640 * 1024;  // 640MB (value is multiplied by 1024)

    private final DirLister rfs;
    private final Connection connection;
    private final SqlStatements sqlStatements;
    private final ExecutorService metadataLoaders;

    public RemoteFileScanner(Map<String, String> settings) throws
            IOException,
            ClassNotFoundException, SQLException {
        final int maxMemUsage;
        final String maxMem = settings.get("sqlite-max-mem");

        if (maxMem != null) {
            maxMemUsage = Integer.parseInt(maxMem);
        } else {
            maxMemUsage = DEFAULT_MAX_MEM_USAGE;
        }

        final File dbFile = new File(settings.get("database-location"));

        /* initialize DB */
        Class.forName("org.h2.Driver");
        final String dbSettings = ";CACHE_SIZE=" + maxMemUsage;
        this.connection = DriverManager.getConnection("jdbc:h2:" + dbFile + dbSettings);
        SqlStatements.execute_CREATE_DATASET_TBL(this.connection);
        SqlStatements.execute_CREATE_DATASET_IDX(this.connection);
        SqlStatements.execute_CREATE_SHARDS_TBL(this.connection);
        SqlStatements.execute_CREATE_SHARDS_START_TIME_IDX(this.connection);
        SqlStatements.execute_CREATE_SHARDS_END_TIME_IDX(this.connection);
        SqlStatements.execute_CREATE_SHARDS_START_END_TIME_IDX(this.connection);
        SqlStatements.execute_CREATE_FIELD_NAMES_TBL(this.connection);
        SqlStatements.execute_CREATE_FIELD_NAMES_IDX(this.connection);
        SqlStatements.execute_CREATE_FIELD_IDS_TBL(this.connection);
        SqlStatements.execute_CREATE_FIELD_IDS_IDX(this.connection);
        this.sqlStatements = new SqlStatements(this.connection);

        /* load remote file system reader */
        this.rfs = new HdfsDirLister(null);

        this.metadataLoaders = Executors.newFixedThreadPool(8);
    }

    /*
     * Only scan these datasets
     */
    public void scanRemoteFiles(final String... datasets) throws IOException, InterruptedException {
        final RemoteIterator<String> datasetsIter;
        datasetsIter = new RemoteIterator<String>() {
            int i = 0;

            @Override
            public boolean hasNext() throws IOException {
                return i < datasets.length;
            }

            @Override
            public String next() throws IOException {
                final String result;

                result = datasets[i];
                i++;
                return result;
            }
        };
        scanRemoteFilesInternal(datasetsIter);
        this.metadataLoaders.shutdown();
        final boolean completed = this.metadataLoaders.awaitTermination(12, TimeUnit.HOURS);
        if (completed)
            System.out.println("done!!!!");
        else
            System.out.println("Timeout while waiting");
    }

    public void scanRemoteFiles() throws IOException {
        final RemoteIterator<String> datasets = getDatasetIterator();
        scanRemoteFilesInternal(datasets);
    }

    private void scanRemoteFilesInternal(RemoteIterator<String> datasets) throws IOException {
        final ShardInfo shard = new ShardInfo();

        while (datasets.hasNext()) {
            final String datasetName = datasets.next();
            final int datasetId;

            try {
                datasetId = getDatasetId(datasetName);
            } catch (SQLException e) {
                throw new IOException(e);
            }

            try {
                final RemoteIterator<String> shards = getShardsForDataset(datasetName);
                boolean first = true;
                while (shards.hasNext()) {
                    final String shardName = shards.next();

                    if (first) {
                        if ("deleted".equals(shardName)) {
                            untrackDataset(datasetId);
                            break;
                        }
                        first = false;
                    }

                    try {
                        shard.reset(shardName);
                    } catch (ShardInfo.ShardNameParseException e) {
                        throw new RuntimeException(e);
                    }
                    track(datasetId, datasetName, shard);
                }
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

    private RemoteIterator<String> getShardsForDataset(String datasetName) throws IOException {
        return this.rfs.listFilesAndDirectories(datasetName);
    }

    private RemoteIterator<String> getDatasetIterator() throws IOException {
        return this.rfs.listFilesAndDirectories(null);
    }

    private int getDatasetId(String datasetName) throws SQLException {
        final int id = sqlStatements.execute_SELECT_DATASET_ID_STATEMENT(datasetName);
        if (id == -1) {
            return sqlStatements.execute_INSERT_DATASET_NAME_STATEMENT(datasetName);
        }
        return id;
    }

    private void track(int datasetId, String datasetName, ShardInfo shard) throws SQLException, IOException {
        final ArrayList<ShardInfo> existingInfos;

        existingInfos = sqlStatements.execute_SELECT_SHARD_STATEMENT(datasetId,
                                                                     shard.startTime,
                                                                     shard.endTime);
        for (ShardInfo si : existingInfos) {
            if (shard.version < si.version) {
                /* already have a more recent version, skip this shard */
                return;
            }
        }
        final int shardId;
        shardId = sqlStatements.execute_INSERT_SHARD_STATEMENT(datasetId,
                                                               shard.startTime,
                                                               shard.endTime,
                                                               shard.version,
                                                               shard.format);

        addShardToQueue(datasetName, shard.filename, shardId);
    }

    private void addShardToQueue(String datasetName, String shardName, int shardId) throws IOException {
        final String metadataPath = datasetName + "/" + shardName + "/" + "metadata.txt";
        this.metadataLoaders.execute(new MetadataScanner(shardId,
                                                        this.rfs.getInputStream(metadataPath, 0, -1)));
    }

    private void untrackDataset(int datasetId) throws SQLException {
        sqlStatements.execute_DELETE_DATASET_SHARDS_STATEMENT(datasetId);
        sqlStatements.execute_DELETE_DATASET_STATEMENT(datasetId);
    }


    static class MetadataScanner implements Runnable {
        static final Map<String, String> config = ImmutableMap.of("sqlite-max-mem",
                                                                  "500",
                                                                  "database-location",
                                                                  "/tmp/3ndTry");
        private static final ThreadLocal<SqarManager> sqarManager = new ThreadLocal<SqarManager>() {
            @Override
            protected SqarManager initialValue() {
                try {
                    return new SqarManager(config);
                } catch (ClassNotFoundException | SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        };

        private final int sqarId;
        private final InputStream meatadata;

        MetadataScanner(int sqarId, InputStream meatadata) {
            this.sqarId = sqarId;
            this.meatadata = meatadata;
        }

        @Override
        public void run() {
            try {
                sqarManager.get().cacheMetadataInternal(sqarId, meatadata);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
