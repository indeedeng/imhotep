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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.indeed.imhotep.ZkEndpointPersister;
import com.indeed.imhotep.client.*;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.shardmaster.utils.SQLWriteManager;
import com.indeed.imhotep.shardmasterrpc.MultiplexingRequestHandler;
import com.indeed.imhotep.shardmasterrpc.RequestMetricStatsEmitter;
import com.indeed.imhotep.shardmasterrpc.RequestResponseServer;
import com.indeed.imhotep.shardmasterrpc.ShardMasterExecutors;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.joda.time.Duration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * @author kenh
 */

public class ShardMasterDaemon {
    private static final Logger LOGGER = Logger.getLogger(ShardMasterDaemon.class);
    private final Config config;
    private volatile RequestResponseServer server;
    private String leaderId;
    private int deleteCounter = 0;

    public ShardMasterDaemon(final Config config) {
        this.config = config;
    }

    public void run() throws IOException, InterruptedException, KeeperException, SQLException {

        LOGGER.info("Initializing fs");
        RemoteCachingFileSystemProvider.newFileSystem();

        LOGGER.info("Starting daemon...");

        final ExecutorService executorService = config.createExecutorService();
        final Timer hostReloadTimer = new Timer(DatasetShardRefresher.class.getSimpleName());
        final ScheduledExecutorService datasetReloadExecutor = Executors.newSingleThreadScheduledExecutor();

        final HostsReloader hostsReloader;
        if (config.hasHostsOverride()) {
            hostsReloader = config.createStaticHostReloader();
        } else {
            hostsReloader = new CheckpointedHostsReloader(
                    new File(config.getHostsFile()),
                    config.createZkHostsReloader(),
                    config.getHostsDropThreshold());
        }

        ZooKeeperConnection zkConnection = new ZooKeeperConnection(config.zkNodes, 30000);
        zkConnection.connect();
        zkConnection.createIfNotExists(config.shardMastersZkPath+"-election", new byte[0], CreateMode.PERSISTENT);
        String leaderElectionRoot = config.shardMastersZkPath+"-election";
        Connection dbConnection = config.getMetadataConnection();
        SQLWriteManager sqlWriteManager = new SQLWriteManager();

        ShardData shardData = new ShardData();

        try (Closer closer = Closer.create()) {
            final Path dataSetsDir = Paths.get(RemoteCachingFileSystemProvider.URI);

            LOGGER.info("Reloading all daemon hosts");
            hostsReloader.run();

            final File file = new File(System.getProperty("imhotep.fs.config.file"));
            final Properties properties = new Properties();
            properties.load(new FileInputStream(file));
            String rootURI = properties.getProperty("imhotep.fs.filestore.hdfs.root.uri");

            final AtomicBoolean shardScanComplete = new AtomicBoolean(false);

            final org.apache.hadoop.fs.Path tmpPath = new org.apache.hadoop.fs.Path(rootURI + "/" + dataSetsDir.toString());
            final DatasetShardRefresher refresher = new DatasetShardRefresher(
                    tmpPath,
                    dbConnection,
                    rootURI,
                    config.shardFilter,
                    shardData,
                    sqlWriteManager,
                    shardScanComplete
            );

            LOGGER.info("Initializing all shard assignments");


            hostReloadTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    hostsReloader.run();
                }
            }, config.getHostsRefreshInterval().getMillis(), config.getHostsRefreshInterval().getMillis());

            datasetReloadExecutor.scheduleAtFixedRate(() -> refresher.run(isLeader(leaderElectionRoot, zkConnection), ++deleteCounter % config.deletePeriod == 0), 0, config.getRefreshInterval().getMillis(), TimeUnit.MILLISECONDS);

            server = new RequestResponseServer(config.getServicePort(), new MultiplexingRequestHandler(
                    config.statsEmitter,
                    new DatabaseShardMaster(config.createAssigner(), shardScanComplete, shardData, hostsReloader),
                    config.shardsResponseBatchSize
            ), config.serviceConcurrency);
            try (ZkEndpointPersister endpointPersister = getZKEndpointPersister()
            ) {
                LOGGER.info("Starting service");
                server.run();
            } finally {
                LOGGER.info("shutting down service");
                server.close();
            }
        } finally {
            hostReloadTimer.cancel();
            datasetReloadExecutor.shutdown();
            executorService.shutdown();
            hostsReloader.shutdown();
            zkConnection.close();
            dbConnection.close();
        }
    }

    public boolean isLeader(final String leaderElectionRoot, final ZooKeeperConnection zkConnection){
        try {
            if(!zkConnection.isConnected()) {
                zkConnection.connect();
                final String leaderPath = zkConnection.create(leaderElectionRoot+"/_", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                leaderId = leaderPath.substring(leaderElectionRoot.length()+1);
            } else if(leaderId == null) {
                final String leaderPath = zkConnection.create(leaderElectionRoot+"/_", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                leaderId = leaderPath.substring(leaderElectionRoot.length()+1);
            }

            final List<String> children = zkConnection.getChildren(leaderElectionRoot, false);
            Collections.sort(children);

            int index = Collections.binarySearch(children, leaderId);

            if(index == 0) {
                LOGGER.info("I am the leader");
                return true;
            }

            if(index == -1) {
                LOGGER.info("Lost my leader path. Resetting my connection to zookeeper.");
                zkConnection.delete(leaderElectionRoot + leaderId, -1);
                zkConnection.close();
            }

            return false;

        } catch (InterruptedException | KeeperException | IOException e ) {
            LOGGER.error(e.getMessage(), e.getCause());
            return false;
        }
    }

    private ZkEndpointPersister getZKEndpointPersister() throws IOException, InterruptedException, KeeperException {
        if(config.zkNodes == null || config.shardMastersZkPath == null) {
            LOGGER.info("Not registering in ZooKeeper as not configured for it");
            return null;
        }
        return new ZkEndpointPersister(config.zkNodes, config.shardMastersZkPath,
                new Host(InetAddress.getLocalHost().getCanonicalHostName(), server.getActualPort()));
    }

    @VisibleForTesting
    void shutdown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    static class Config {
        private String zkNodes;
        private String imhotepDaemonsZkPath;
        private String shardMastersZkPath;
        private String dbFile;
        private String dbParams = "MULTI_THREADED=TRUE;CACHE_SIZE=" + (1024 * 1024);
        private String hostsFile;
        private String hostsOverride;
        private String disabledHosts;
        private String shardAssigner;
        private ShardFilter shardFilter = ShardFilter.ACCEPT_ALL;
        private int servicePort = 0;
        private int serviceConcurrency = 10;
        private int shardsResponseBatchSize = 1000;
        private int threadPoolSize = 5;
        private int replicationFactor = 2;
        private Duration refreshInterval = Duration.standardMinutes(5);
        private Duration stalenessThreshold = Duration.standardMinutes(15);
        private Duration hostsRefreshInterval = Duration.standardMinutes(1);
        private double hostsDropThreshold = 0.5;
        private RequestMetricStatsEmitter statsEmitter = RequestMetricStatsEmitter.NULL_EMITTER;
        private String metadataDBURL;
        private int deletePeriod = 200;

        public Config setMetadataDBURL(final String url){
            this.metadataDBURL = url;
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

        public Config setDbParams(final String dbParams) {
            this.dbParams = dbParams;
            return this;
        }

        public Config setHostsFile(final String hostsFile) {
            this.hostsFile = hostsFile;
            return this;
        }

        public Config setHostsOverride(final String hostsOverride) {
            this.hostsOverride = hostsOverride;
            return this;
        }

        public Config setDisabledHosts(final String disabledHosts) {
            this.disabledHosts = disabledHosts;
            return this;
        }

        public Config setShardAssigner(String shardAssigner) {
            this.shardAssigner = shardAssigner;
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

        public Config setServiceConcurrency(final int serviceConcurrency) {
            this.serviceConcurrency = serviceConcurrency;
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

        public Config setHostsRefreshInterval(final long hostsRefreshInterval) {
            this.hostsRefreshInterval = Duration.millis(hostsRefreshInterval);
            return this;
        }

        public Config setHostsDropThreshold(final double hostsDropThreshold) {
            this.hostsDropThreshold = hostsDropThreshold;
            return this;
        }

        public Config setStatsEmitter(final RequestMetricStatsEmitter statsEmitter) {
            this.statsEmitter = statsEmitter;
            return this;
        }

        boolean hasHostsOverride() {
            return StringUtils.isNotBlank(hostsOverride);
        }

        HostsReloader createZkHostsReloader() {
            Preconditions.checkNotNull(zkNodes, "ZooKeeper nodes config is missing");
            return new ZkHostsReloader(zkNodes, imhotepDaemonsZkPath, false);
        }

        HostsReloader createStaticHostReloader() throws IOException {
            Preconditions.checkNotNull(hostsOverride, "Static hosts config is missing");
            final List<Host> hostList = Lists.newArrayList(parseHostsList(hostsOverride));
            if (StringUtils.isNotBlank(disabledHosts)) {
                final Set<Host> disabledHostList = Sets.newHashSet(parseHostsList(disabledHosts));
                for(int i = 0; i < hostList.size(); i++) {
                    if(disabledHostList.contains(hostList.get(i))) {
                        hostList.set(i, null);
                    }
                }
            }
            return new DummyHostsReloader(hostList);
        }

        private List<Host> parseHostsList(String value) {
            final ImmutableList.Builder<Host> hostsBuilder = ImmutableList.builder();
            for (final String hostString : value.split(",")) {
                try {
                    final Host host = Host.valueOf(hostString.trim());
                    hostsBuilder.add(host);
                } catch (final IllegalArgumentException e) {
                    LOGGER.warn("Failed to parse host " + hostString + ". Ignore it.", e);
                }
            }
            return hostsBuilder.build();
        }

        ExecutorService createExecutorService() {
            return ShardMasterExecutors.newBlockingFixedThreadPool(threadPoolSize);
        }

        HikariDataSource createDataSource() {
            final HikariConfig dbConfig = new HikariConfig();
            // this is a bit arbitrary but we need to ensure that the pool size is large enough for the # of threads
            dbConfig.setMaximumPoolSize(Math.max(10, threadPoolSize + serviceConcurrency + 5));

            String url;
            if (dbFile != null) {
                url = "jdbc:h2:" + dbFile;
            } else {
                url = "jdbc:h2:mem:shardassignment";
            }

            if (dbParams != null) {
                url += ";" + dbParams;
            }
            dbConfig.setJdbcUrl(url);
            return new HikariDataSource(dbConfig);
        }

        int getServicePort() {
            return servicePort;
        }

        ShardFilter getShardFilter() {
            return shardFilter;
        }

        Connection getMetadataConnection() throws SQLException {
            return DriverManager.getConnection(metadataDBURL);
        }

        ShardAssigner createAssigner() {
            String shardAssignerToUse = shardAssigner;
            // Provide a default if not configured
            if(Strings.isNullOrEmpty(shardAssignerToUse)) {
                shardAssignerToUse = replicationFactor == 1 ? "time" : "minhash";
            }

            if("time".equals(shardAssignerToUse)) {
                if (replicationFactor != 1) {
                    throw new IllegalArgumentException("Time shard assigner only supports replication factor = 1 now. " +
                            "'minhash' shard assigner is recommended if higher replication factor is used.");
                }
                return new TimeBasedShardAssigner();
            } else if ("minhash".equals(shardAssignerToUse)){
                return new MinHashShardAssigner(replicationFactor);
            } else {
                throw new IllegalArgumentException("shardAssigner config value must be set to 'minhash' or 'time'");
            }
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

        String getDbFile() {
            return dbFile;
        }

        Duration getHostsRefreshInterval() {
            return hostsRefreshInterval;
        }

        double getHostsDropThreshold() {
            return hostsDropThreshold;
        }

        public int getDeletePeriod() {
            return deletePeriod;
        }

        public void setDeletePeriod(int deletePeriod) {
            this.deletePeriod = deletePeriod;
        }
    }

    public static void main(final String[] args) throws InterruptedException, IOException, KeeperException, SQLException {
        new ShardMasterDaemon(new Config()
                .setZkNodes(System.getProperty("imhotep.shardmaster.zookeeper.nodes"))
                .setDbFile(System.getProperty("imhotep.shardmaster.db.file"))
                .setHostsFile(System.getProperty("imhotep.shardmaster.hosts.file"))
                .setServicePort(Integer.parseInt(System.getProperty("imhotep.shardmaster.server.port")))
        ).run();
    }
}
