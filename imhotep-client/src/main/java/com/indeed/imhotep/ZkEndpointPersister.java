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

package com.indeed.imhotep;

import com.indeed.imhotep.client.Host;
import com.indeed.util.core.threads.NamedThreadFactory;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Helpful utility to bookmark a service's endpoint in zookeeper for the duration of its lifetime
 * @author kenh
 */

public class ZkEndpointPersister implements Watcher, Closeable {
    private static final Logger LOG = Logger.getLogger(ZkEndpointPersister.class);
    private static final int TIMEOUT = 60000;
    private final String zkNodes;
    private final String rootPath;
    private final Host endpoint;
    private final ExecutorService executorService;
    private ZooKeeperConnection connection;
    private String zkPath;

    public ZkEndpointPersister(final String zkNodes, final String rootPath, final Host endpoint) throws KeeperException, InterruptedException, IOException {
        this.zkNodes = zkNodes;
        this.rootPath = rootPath;
        this.endpoint = endpoint;
        executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory(getClass().getSimpleName()));

        connectAndPersist();
    }

    private synchronized void connectAndPersist() throws KeeperException, InterruptedException, IOException {
        closeZkConn();

        connection = new ZooKeeperConnection(zkNodes, TIMEOUT);
        connection.connect();
        connection.register(this);

        deletePath();
        final byte[] data = String.format("%s:%d", endpoint.getHostname(), endpoint.getPort()).getBytes();
        zkPath = new File(rootPath, endpoint.getHostname() + "-" + UUID.randomUUID().toString()).getPath();
        connection.createFullPath(
                zkPath,
                data, CreateMode.EPHEMERAL);
    }

    @Override
    public void process(final WatchedEvent watchedEvent) {
        switch (watchedEvent.getState()) {
            case Expired:
            case Disconnected:
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            connectAndPersist();
                        } catch (final KeeperException|InterruptedException|IOException e) {
                            throw new IllegalStateException("Failed to renew zookeeper connection", e);
                        }
                    }
                });
                break;
        }
    }

    private synchronized void deletePath() {
        if ((connection != null) && (zkPath != null)) {
            try {
                final Stat pathStats = connection.exists(zkPath, true);
                if (pathStats != null) {
                    connection.delete(zkPath, pathStats.getVersion());
                }
            } catch (final InterruptedException|KeeperException e) {
                LOG.error("failed to delete zk path " + zkPath, e);
            }
        }
    }

    private synchronized void closeZkConn() {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (final InterruptedException e) {
                throw new IllegalStateException("Failed to close zookeeper connection", e);
            }
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
        deletePath();
        closeZkConn();
    }
}
