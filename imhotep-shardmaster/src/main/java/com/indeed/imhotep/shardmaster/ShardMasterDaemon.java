package com.indeed.imhotep.shardmaster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.indeed.imhotep.ZkEndpointPersister;
import com.indeed.imhotep.client.CheckpointedHostsReloader;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.client.ZkHostsReloader;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.fs.sql.SchemaInitializer;
import com.indeed.imhotep.shardmaster.db.shardinfo.Tables;
import com.indeed.imhotep.shardmaster.rpc.MultiplexingRequestHandler;
import com.indeed.imhotep.shardmaster.rpc.RequestResponseServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.joda.time.Duration;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * @author kenh
 */

public class ShardMasterDaemon {
    private static final Logger LOGGER = Logger.getLogger(ShardMasterDaemon.class);
    private final Config config;
    private volatile RequestResponseServer server;

    public ShardMasterDaemon(final Config config) {
        this.config = config;
    }

    public void run() throws IOException, ExecutionException, InterruptedException, KeeperException, SQLException {

        LOGGER.info("Initializing fs");
        if (config.getFsConfigFile() != null) {
            RemoteCachingFileSystemProvider.newFileSystem(new File(config.getFsConfigFile()));
        } else {
            RemoteCachingFileSystemProvider.newFileSystem();
        }

        LOGGER.info("Starting daemon...");

        final ExecutorService executorService = config.createExecutorService();
        final Timer timer = new Timer(DatasetShardAssignmentRefresher.class.getSimpleName());
        try (HikariDataSource dataSource = config.createDataSource()) {
            new SchemaInitializer(dataSource).initialize(Collections.singletonList(Tables.TBLSHARDASSIGNMENTINFO));

            final ShardAssignmentInfoDao shardAssignmentInfoDao = new ShardAssignmentInfoDao(dataSource, config.getStalenessThreshold());

            final RemoteCachingPath dataSetsDir = (RemoteCachingPath) Paths.get(RemoteCachingFileSystemProvider.URI);

            LOGGER.info("Reloading all daemon hosts");
            final HostsReloader hostsReloader = new CheckpointedHostsReloader(
                    new File(config.getHostsFile()),
                    config.createHostsReloader(),
                    config.getHostsDropThreshold());

            hostsReloader.run();

            final DatasetShardAssignmentRefresher refresher = new DatasetShardAssignmentRefresher(
                    dataSetsDir,
                    config.getShardFilter(),
                    executorService,
                    hostsReloader,
                    config.createAssigner(),
                    shardAssignmentInfoDao
            );

            LOGGER.info("Initializing all shard assignments");
            refresher.initialize().getAllShards().get();

            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    hostsReloader.run();
                }
            }, Duration.standardMinutes(1).getMillis(), Duration.standardMinutes(1).getMillis());

            timer.schedule(refresher, config.getRefreshInterval().getMillis(), config.getRefreshInterval().getMillis());

            server = new RequestResponseServer(config.getServicePort(), new MultiplexingRequestHandler(
                    new DatabaseShardMaster(shardAssignmentInfoDao),
                    config.shardsResponseBatchSize
            ));
            try (ZkEndpointPersister endpointPersister = new ZkEndpointPersister(config.zkNodes, config.shardMastersZkPath,
                         new Host(InetAddress.getLocalHost().getCanonicalHostName(), server.getActualPort()))
            ) {
                LOGGER.info("Starting service");
                server.run();
            } finally {
                LOGGER.info("shutting down service");
                server.close();
            }
        } finally {
            timer.cancel();
            executorService.shutdown();
        }
    }

    @VisibleForTesting
    void shutdown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    static class Config {
        private String fsConfigFile;
        private String zkNodes;
        private String imhotepDaemonsZkPath;
        private String shardMastersZkPath;
        private String dbFile;
        private String hostsFile;
        private ShardFilter shardFilter = ShardFilter.ACCEPT_ALL;
        private int servicePort = 0;
        private int shardsResponseBatchSize = 1000;
        private int threadPoolSize = 5;
        private int replicationFactor = 3;
        private Duration refreshInterval = Duration.standardMinutes(15);
        private Duration stalenessThreshold = Duration.standardHours(1);
        private double hostsDropThreshold = 0.5;

        public Config setFsConfigFile(final String fsConfigFile) {
            this.fsConfigFile = fsConfigFile;
            return this;
        }

        public Config setZkNodes(final String zkNodes) {
            this.zkNodes = zkNodes;
            return this;
        }

        public Config setImhotepDaemonsZkPath(final String imhotepDaemonsZkPath) {
            this.imhotepDaemonsZkPath = imhotepDaemonsZkPath;
            return this;
        }

        public Config setShardMastersZkPath(final String shardMastersZkPath) {
            this.shardMastersZkPath = shardMastersZkPath;
            return this;
        }

        public Config setDbFile(final String dbFile) {
            this.dbFile = dbFile;
            return this;
        }

        public Config setHostsFile(final String hostsFile) {
            this.hostsFile = hostsFile;
            return this;
        }

        public Config setShardFilter(final ShardFilter shardFilter) {
            this.shardFilter = shardFilter;
            return this;
        }

        public Config setServicePort(final int servicePort) {
            this.servicePort = servicePort;
            return this;
        }

        public Config setShardsResponseBatchSize(final int shardsResponseBatchSize) {
            this.shardsResponseBatchSize = shardsResponseBatchSize;
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
            this.refreshInterval = Duration.millis(refreshInterval);
            return this;
        }

        public Config setStalenessThreshold(final long stalenessThreshold) {
            this.stalenessThreshold = Duration.millis(stalenessThreshold);
            return this;
        }

        public Config setHostsDropThreshold(final double hostsDropThreshold) {
            this.hostsDropThreshold = hostsDropThreshold;
            return this;
        }

        HostsReloader createHostsReloader() {
            Preconditions.checkNotNull(zkNodes, "ZooKeeper nodes config is missing");
            return new ZkHostsReloader(zkNodes, imhotepDaemonsZkPath, false);
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

        String getFsConfigFile() {
            return fsConfigFile;
        }

        int getServicePort() {
            return servicePort;
        }

        ShardFilter getShardFilter() {
            return shardFilter;
        }

        ShardAssigner createAssigner() {
            return new MinHashShardAssigner(replicationFactor);
        }

        Duration getRefreshInterval() {
            return refreshInterval;
        }

        Duration getStalenessThreshold() {
            return stalenessThreshold;
        }

        String getHostsFile() {
            Preconditions.checkNotNull(hostsFile, "HostsFile config is missing");
            return hostsFile;
        }

        double getHostsDropThreshold() {
            return hostsDropThreshold;
        }
    }

    public static void main(final String[] args) throws InterruptedException, ExecutionException, IOException, KeeperException, SQLException {
        new ShardMasterDaemon(new Config()
                .setZkNodes(System.getProperty("imhotep.shardmaster.zookeeper.nodes"))
                .setDbFile(System.getProperty("imhotep.shardmaster.db.file"))
                .setHostsFile(System.getProperty("imhotep.shardmaster.hosts.file"))
                .setServicePort(Integer.parseInt(System.getProperty("imhotep.shardmaster.server.port")))
        ).run();
    }
}
