package com.indeed.imhotep.shardmanager;

import com.google.common.base.Preconditions;
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.client.ZkHostsReloader;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.shardmanager.rpc.MultiplexingRequestHandler;
import com.indeed.imhotep.shardmanager.rpc.RequestResponseServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Timer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * @author kenh
 */

public class ShardManagerDaemon {
    private static final Logger LOGGER = Logger.getLogger(ShardManagerDaemon.class);
    final Config config;

    public ShardManagerDaemon(final Config config) {
        this.config = config;
    }

    public void run() throws IOException, ExecutionException, InterruptedException {
        LOGGER.info("Starting daemon...");

        final ExecutorService executorService = config.createExecutorService();
        final Timer timer = new Timer();
        try (HikariDataSource dataSource = config.createDataSource()) {
            final ShardAssignmentInfoDao shardAssignmentInfoDao = new ShardAssignmentInfoDao(dataSource);

            final RemoteCachingPath dataSetsDir = (RemoteCachingPath) Paths.get(RemoteCachingFileSystemProvider.URI);

            LOGGER.info("Reloading all daemon hosts");
            final HostsReloader hostReloader = config.createHostReloader();

            final DatasetShardAssignmentRefresher refresher = new DatasetShardAssignmentRefresher(
                    dataSetsDir,
                    config.getShardFilter(),
                    executorService,
                    hostReloader,
                    config.createAssigner(),
                    shardAssignmentInfoDao
            );

            LOGGER.info("Initializing all shard assignments");
            refresher.initialize();

            timer.schedule(refresher, config.getRefreshInterval(), config.getRefreshInterval());

            try (RequestResponseServer server = new RequestResponseServer(config.getServerPort(), new MultiplexingRequestHandler(
                    new ShardManagerServer(shardAssignmentInfoDao)
            ))) {
                LOGGER.info("Starting request response server");
                server.run();
            }
        } finally {
            timer.cancel();
            executorService.shutdown();
        }
    }

    static class Config {
        String zkNodes;
        String dbFile;
        private ShardFilter shardFilter = ShardFilter.ACCEPT_ALL;
        int serverPort = 0;
        int threadPoolSize = 5;
        int replicationFactor = 3;
        long refreshInterval = 15 * 60 * 1000;

        public Config setZkNodes(final String zkNodes) {
            this.zkNodes = zkNodes;
            return this;
        }

        public Config setDbFile(final String dbFile) {
            this.dbFile = dbFile;
            return this;
        }

        public Config setShardFilter(final ShardFilter shardFilter) {
            this.shardFilter = shardFilter;
            return this;
        }

        public Config setServerPort(final int serverPort) {
            this.serverPort = serverPort;
            return this;
        }

        public Config setThreadPoolSize(final int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Config setReplicationFactor(final int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public Config setRefreshInterval(final long refreshInterval) {
            this.refreshInterval = refreshInterval;
            return this;
        }

        HostsReloader createHostReloader() {
            Preconditions.checkNotNull(zkNodes, "ZooKeeper nodes config is missing");
            return new ZkHostsReloader(zkNodes, true);
        }

        ExecutorService createExecutorService() {
            return ScanWorkExecutors.newBlockingFixedThreadPool(threadPoolSize);
        }

        HikariDataSource createDataSource() {
            Preconditions.checkNotNull(dbFile, "DBFile config is missing");
            final HikariConfig dbConfig = new HikariConfig();
            // this is a bit arbitrary but we need to ensure that the pool size is large enough for the # of threads
            dbConfig.setMaximumPoolSize(Math.max(10, threadPoolSize + 1));
            dbConfig.setJdbcUrl("jdbc:h2:" + dbFile);
            return new HikariDataSource(dbConfig);
        }

        int getServerPort() {
            return serverPort;
        }

        ShardFilter getShardFilter() {
            return shardFilter;
        }

        ShardAssigner createAssigner() {
            return new MinHashShardAssigner(replicationFactor);
        }

        long getRefreshInterval() {
            return refreshInterval;
        }
    }

    public static void main(final String[] args) throws InterruptedException, ExecutionException, IOException {
        RemoteCachingFileSystemProvider.newFileSystem();

        new ShardManagerDaemon(new Config()
                .setZkNodes(System.getProperty("imhotep.shardmanager.zookeeper.nodes"))
                .setDbFile(System.getProperty("imhotep.shardmanager.db.file"))
                .setServerPort(Integer.parseInt(System.getProperty("imhotep.shardmanager.server.port")))
        ).run();
    }
}
