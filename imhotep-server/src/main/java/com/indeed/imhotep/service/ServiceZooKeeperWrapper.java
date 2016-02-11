/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ServiceZooKeeperWrapper implements Closeable, Runnable, Watcher {

    private static final Logger log = Logger.getLogger(ServiceZooKeeperWrapper.class);

    private static final int TIMEOUT = 60000;

    private final String rootPath, nodeName;
    private final byte[] data;

    private final ZooKeeperConnection zkConnection;

    private final AtomicBoolean   closed   = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public ServiceZooKeeperWrapper(final String zkNodes,
                                   final String hostname,
                                   final int port,
                                   final String rootPath) {
        zkConnection  = new ZooKeeperConnection(zkNodes, TIMEOUT);
        this.rootPath = rootPath;
        nodeName      = UUID.randomUUID().toString();
        data          = (hostname + ":" + port).getBytes(Charsets.UTF_8);

        try {
            Future<?> future = executor.submit(this);
            if (future.get() != null) {
                throw new RuntimeException("failed to execute ServiceZooKeeperWrapper task");
            }
        } catch (Exception ex) {
            log.error("failed to create ServiceZooKeeperWrapper", ex);
            try {
                close();
            }
            catch (IOException ioex) {
                log.error("failed to close ServiceZooKeeperWrapper", ioex);
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (closed.compareAndSet(false, true)) {
                zkConnection.close();
            }
        } catch (InterruptedException e) {
            log.error("failed to close ZooKeeper connection", e);
        }
    }

    public boolean isAlive() {
        return zkConnection.getState().isAlive();
    }

    public void run() {
        final String location = ServiceZooKeeperWrapper.class.getSimpleName();
        final int maxAttemptsBeforeReconnect = 5;
        int     attempt   = 0;
        boolean connected = false;
        boolean created   = false;
        while (!created && !closed.get()) {
            ++attempt;
            log.info(location + " run()" +
                     " attempt: " + attempt + "/" + maxAttemptsBeforeReconnect +
                     " created: " + created + " closed: " + closed.get());
            try {
                try {
                    if (!connected || (attempt % maxAttemptsBeforeReconnect == 0)) {
                        log.info(location + " reconnecting connected: " + connected);
                        /* Note that ZooKeeperConnection blocks until it sees a
                           SyncConnected state (unlike ZooKeeper::connect(), which is
                           asynchronous. */
                        zkConnection.connect();
                        zkConnection.register(this);
                        connected = true;
                    }
                    /* Note that ZooKeeperConnection only creates these if they do
                       not already exist. So there's no harm in executing these
                       multiple times in the face of transient failures. */
                    log.info(location + " creating paths attempt: " + attempt);
                    zkConnection.createFullPath(rootPath, new byte[0], CreateMode.PERSISTENT, true);
                    zkConnection.create(rootPath + "/" + nodeName, data,
                                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    created = true;
                }
                catch (Exception ex) {
                    log.error("failed to create ZooKeeper entries attempt: " + attempt, ex);
                    Thread.sleep(TIMEOUT);
                }
            }
            catch (InterruptedException ex) {
                log.error("interrupted whilst creating ZooKeeper entries attempt: " + attempt, ex);
            }
        }
    }

    public void process(WatchedEvent watchedEvent) {

        final String location = ServiceZooKeeperWrapper.class.getSimpleName();
        log.info(location + " process() closed: " + closed.get() +
                 " watchedEvent.getType(): " + watchedEvent.getType() +
                 " watchedEvent.getState(): " + watchedEvent.getState());

        if (closed.get()) return;

        if (watchedEvent.getType() == Event.EventType.None &&
            watchedEvent.getState() == Event.KeeperState.Expired) {
            try {
                log.info(location + " process() closing ZooKeeper connection");
                zkConnection.close();
            } catch (InterruptedException ex) {
                log.error("failed to close connection", ex);
            }
            executor.submit(this);
        }
    }
}
