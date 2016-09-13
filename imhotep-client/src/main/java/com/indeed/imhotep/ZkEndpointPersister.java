package com.indeed.imhotep;

import com.indeed.imhotep.client.Host;
import com.indeed.util.core.threads.NamedThreadFactory;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

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
    private static final int TIMEOUT = 60000;
    private final String zkNodes;
    private final String rootPath;
    private final Host endpoint;
    private final ExecutorService executorService;
    private ZooKeeperConnection connection;

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

        final byte[] data = String.format("%s:%d", endpoint.getHostname(), endpoint.getPort()).getBytes();
        connection.createFullPath(
                new File(rootPath, endpoint.getHostname() + "-" + UUID.randomUUID().toString()).getPath(),
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
        closeZkConn();
    }
}
