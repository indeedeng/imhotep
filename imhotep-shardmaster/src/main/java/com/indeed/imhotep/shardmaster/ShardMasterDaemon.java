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
import com.google.common.io.Closer;
import com.indeed.imhotep.ZkEndpointPersister;
import com.indeed.imhotep.client.*;
import com.indeed.imhotep.hadoopcommon.KerberosUtils;
import com.indeed.imhotep.shardmaster.utils.SQLWriteManager;
import com.indeed.imhotep.shardmasterrpc.MultiplexingRequestHandler;
import com.indeed.imhotep.shardmasterrpc.RequestMetricStatsEmitter;
import com.indeed.imhotep.shardmasterrpc.RequestResponseServer;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import com.indeed.imhotep.shardmasterrpc.ShardMasterExecutors;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.joda.time.Duration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author kenh
 */

public class ShardMasterDaemon {
    private static final Logger LOGGER = Logger.getLogger(ShardMasterDaemon.class);
    private final Config config;
    private volatile RequestResponseServer server;
    private String leaderId;
    private long lastDeleteTimeMillis;
    private Lock startupLock = new ReentrantLock();

    public ShardMasterDaemon(final Config config) {
        this.config = config;
    }

    public void waitForStartup(final long timeout) throws TimeoutException, InterruptedException {
        Thread.sleep(300);
        if(!startupLock.tryLock(timeout, TimeUnit.MILLISECONDS)){
            throw new TimeoutException("ImhotepDaemon failed to start within " + timeout + " ms");
        }

    }

    public void run() throws IOException, InterruptedException, KeeperException {
        startupLock.lock();

        lastDeleteTimeMillis = System.currentTimeMillis();

        LOGGER.info("Starting daemon...");

        // TODO: fix for open source
        KerberosUtils.loginFromKeytab(null, null);

        final ExecutorService executorService = config.createExecutorService();
        final Timer hostReloadTimer = new Timer(ShardRefresher.class.getSimpleName());
        final ScheduledExecutorService datasetReloadExecutor = Executors.newSingleThreadScheduledExecutor();

        final HostsReloader hostsReloader;
        if (config.hasStaticHostsList()) {
            if(config.hasDynamicHostsList()) {
                final HostsReloader zkHostsReloader = config.createZkHostsReloader();
                hostsReloader = new StaticWithDynamicDowntimeHostsReloader(config.getStaticHosts(), zkHostsReloader);
            }
            else {
                hostsReloader = new DummyHostsReloader(config.getStaticHosts());
            }
        } else if(config.hasDynamicHostsList()) {
                hostsReloader = config.createZkHostsReloader();
        } else {
            throw new IllegalArgumentException("At least one of the static hosts list or the zookeeper path has to be set");
        }

        final ZooKeeperConnection zkConnection = config.getLeaderZkConnection();
        final String leaderElectionRoot = config.getLeaderElectionRoot();
        final JdbcTemplate dbConnection = config.getMetadataConnection();
        final SQLWriteManager sqlWriteManager = new SQLWriteManager();

        ShardData shardData = new ShardData();

        try (Closer closer = Closer.create()) {
            final Path shardsRootPath = Paths.get(config.shardsRootPath);

            LOGGER.info("Reloading all daemon hosts");
            hostsReloader.run();

            final org.apache.hadoop.fs.Path shardsRootHDFSPath = new org.apache.hadoop.fs.Path(shardsRootPath.toString());
            final ShardRefresher refresher = new ShardRefresher(
                    shardsRootHDFSPath,
                    dbConnection,
                    config.shardFilter,
                    shardData,
                    sqlWriteManager
            );

            refresher.refresh(config.localMode, !config.localMode, ShardFilter.ACCEPT_ALL.equals(config.shardFilter), false, isLeader(leaderElectionRoot, zkConnection));

            hostReloadTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    hostsReloader.run();
                }
            }, config.getHostsRefreshInterval().getMillis(), config.getHostsRefreshInterval().getMillis());

            datasetReloadExecutor.scheduleAtFixedRate(() -> {
                final boolean shouldDelete = ShardFilter.ACCEPT_ALL.equals(config.shardFilter) && System.currentTimeMillis() - lastDeleteTimeMillis > config.getDeleteInterval().getMillis();
                if(shouldDelete) {
                    lastDeleteTimeMillis = System.currentTimeMillis();
                }
                final boolean leader = isLeader(leaderElectionRoot, zkConnection);
                refresher.refresh(leader || config.localMode, !config.localMode, shouldDelete, leader, leader);
            }, config.getRefreshInterval().getMillis(), config.getRefreshInterval().getMillis(), TimeUnit.MILLISECONDS);

            server = new RequestResponseServer(config.getServerSocket(), new MultiplexingRequestHandler(
                    config.statsEmitter,
                    new DatabaseShardMaster(config.createAssigner(), shardData, hostsReloader, refresher, config.localMode ? "" : ".sqar"),
                    config.shardsResponseBatchSize
            ), config.serviceConcurrency);
            startupLock.unlock();
            try (ZkEndpointPersister endpointPersister = getZKEndpointPersister()) {
                LOGGER.info("Starting service");
                server.run();
            } finally {
                LOGGER.info("shutting down service");
                server.close();
            }
        } catch (Exception e) {
            LOGGER.error("Error during startup", e);
        }
        finally {
            startupLock.unlock();
            hostReloadTimer.cancel();
            datasetReloadExecutor.shutdown();
            executorService.shutdown();
            hostsReloader.shutdown();
            if(zkConnection!=null) {
                zkConnection.close();
            }
        }
    }

    public boolean isLeader(final String leaderElectionRoot, final ZooKeeperConnection zkConnection){
        if(zkConnection == null) {
            return false;
        }

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
        if(config.zkNodes == null || config.shardMastersZkPath == null || config.localMode) {
            LOGGER.info("Not registering in ZooKeeper as not configured for it");
            return null;
        }
        return new ZkEndpointPersister(config.zkNodes, config.shardMastersZkPath,
                new Host(InetAddress.getLocalHost().getCanonicalHostName(), server.getActualPort()));
    }

    @VisibleForTesting
    public void shutdown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    public static class Config {
        private String zkNodes;
        private String imhotepDaemonsZkPath;
        private String shardMastersZkPath;
        private String hostsListStatic;
        private String shardAssigner;
        private ShardFilter shardFilter = ShardFilter.ACCEPT_ALL;
        private int serviceConcurrency = 10;
        private int shardsResponseBatchSize = 1000;
        private int threadPoolSize = 5;
        private int replicationFactor = 2;
        private Duration refreshInterval = Duration.standardMinutes(5);
        private Duration hostsRefreshInterval = Duration.standardMinutes(1);
        private RequestMetricStatsEmitter statsEmitter = RequestMetricStatsEmitter.NULL_EMITTER;
        private String metadataDBURL;
        private Duration deleteInterval = Duration.standardDays(1);
        private String metadataDBUsername;
        private String metadataDBPassword;
        private boolean localMode = false;
        private String shardsRootPath = "hdfs:///var/imhotep";
        private ServerSocket serverSocket;

        public Config setHostsListStatic(String hostsListStatic) {
            this.hostsListStatic = hostsListStatic;
            return this;
        }

        public Config setLocalMode(boolean newLocalMode) {
            this.localMode = newLocalMode;
            return this;
        }

        public Config setMetadataDBUsername(final String username) {
            this.metadataDBUsername = username;
            return this;
        }

        public Config setMetadataDBPassword(final String password) {
            this.metadataDBPassword = password;
            return this;
        }

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

        public Config setShardAssigner(String shardAssigner) {
            this.shardAssigner = shardAssigner;
            return this;
        }

        public Config setShardFilter(final ShardFilter shardFilter) {
            this.shardFilter = shardFilter;
            return this;
        }

        public Config setServerSocket(final ServerSocket servicePort) {
            this.serverSocket = servicePort;
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

        public Config setHostsRefreshInterval(final long hostsRefreshInterval) {
            this.hostsRefreshInterval = Duration.millis(hostsRefreshInterval);
            return this;
        }

        public Config setStatsEmitter(final RequestMetricStatsEmitter statsEmitter) {
            this.statsEmitter = statsEmitter;
            return this;
        }

        public Config setShardsRootPath(final String shardsRootPath) {
            this.shardsRootPath = shardsRootPath;
            return this;
        }

        boolean hasStaticHostsList() {
            return StringUtils.isNotBlank(hostsListStatic);
        }

        boolean hasDynamicHostsList() {
            return StringUtils.isNotBlank(imhotepDaemonsZkPath);
        }

        HostsReloader createZkHostsReloader() {
            Preconditions.checkNotNull(zkNodes, "ZooKeeper nodes config is missing");
            return new ZkHostsReloader(zkNodes, imhotepDaemonsZkPath, false);
        }

        List<Host> getStaticHosts() throws IOException {
            Preconditions.checkNotNull(hostsListStatic, "Static hosts config is missing");
            return Lists.newArrayList(parseHostsList(hostsListStatic));
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

        ShardFilter getShardFilter() {
            return shardFilter;
        }

        JdbcTemplate getMetadataConnection() {
            if(this.localMode) {
                return null;
            }
            BasicDataSource ds = new BasicDataSource();
            ds.setDriverClassName("com.mysql.jdbc.Driver");
            ds.setUrl(metadataDBURL);
            ds.setUsername(metadataDBUsername);
            ds.setPassword(metadataDBPassword);
            ds.setValidationQuery("SELECT 1");
            return new JdbcTemplate(ds);
        }

        ShardAssigner createAssigner() {
            String shardAssignerToUse = shardAssigner;
            // Provide a default if not configured
            if(Strings.isNullOrEmpty(shardAssignerToUse)) {
                shardAssignerToUse = (replicationFactor == 1 && hasStaticHostsList()) ? "time" : "minhash";
            }

            if("time".equals(shardAssignerToUse)) {
                if (replicationFactor != 1) {
                    throw new IllegalArgumentException("Time shard assigner only supports replication factor = 1 now. " +
                            "'minhash' shard assigner is recommended if higher replication factor is used.");
                }
                if (!hasStaticHostsList()) {
                    throw new IllegalArgumentException("Time shard assigner requires host list to be explicitly listed in now. " +
                            "'minhash' shard assigner is recommended if host list is not fixed.");
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

        Duration getHostsRefreshInterval() {
            return hostsRefreshInterval;
        }

        public Duration getDeleteInterval() {
            return deleteInterval;
        }

        public void setDeleteInterval(long deleteIntervalMillis) {
            this.deleteInterval = Duration.millis(deleteIntervalMillis);
        }

        public String getShardsRootPath() {
            return shardsRootPath;
        }

        public ZooKeeperConnection getLeaderZkConnection() throws IOException, InterruptedException, KeeperException {
            if(localMode) {
                return null;
            }
            ZooKeeperConnection zkConnection = new ZooKeeperConnection(this.zkNodes, 30000);
            zkConnection.connect();
            zkConnection.createIfNotExists(this.shardMastersZkPath+"-election", new byte[0], CreateMode.PERSISTENT);
            return zkConnection;
        }

        public String getLeaderElectionRoot() {
            return shardMastersZkPath+"-election";
        }

        public ServerSocket getServerSocket() throws IOException {
            return serverSocket;
        }
    }

    public static void main(final String[] args) throws InterruptedException, IOException, KeeperException, SQLException {
        // TODO: fix this to set correct configs
        new ShardMasterDaemon(new Config()
                .setZkNodes(System.getProperty("imhotep.shardmaster.zookeeper.nodes"))
                .setServerSocket(new ServerSocket(Integer.parseInt(System.getProperty("imhotep.shardmaster.server.port"))))
        ).run();
    }
}
