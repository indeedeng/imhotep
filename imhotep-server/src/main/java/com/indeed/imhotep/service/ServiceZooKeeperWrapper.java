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

import com.google.common.base.Charsets;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.*;

/**
   All real activity happens in run() thread. Other threads can just
   change state and kick that one.
 */
public class ServiceZooKeeperWrapper implements Closeable, Runnable, Watcher {

    private static final Logger log = Logger.getLogger(ServiceZooKeeperWrapper.class);

    private static final int SESSION_TIMEOUT_MILLIS = 60000;
    private static final int ZK_POLL_SECONDS        = 60;

    private final String zkNodes;
    private final String rootPath, fullPath;
    private final byte[] data;

    private final AtomicReference<ZooKeeperConnection> zkRef = new AtomicReference<>();

    private final ScheduledExecutorService scheduler;

    public ServiceZooKeeperWrapper(final String zkNodes,
                                   final String hostname,
                                   final int    port,
                                   final String rootPath) {
        this.zkNodes  = zkNodes;
        this.rootPath = rootPath;
        this.fullPath = rootPath + "/" + UUID.randomUUID().toString();
        this.data     = (hostname + ":" + port).getBytes(Charsets.UTF_8);

        try {
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(this, 1, ZK_POLL_SECONDS, SECONDS);
        }
        catch (RuntimeException ex) {
            log.error("failed to schedule service", ex);
            throw ex;
        }
    }

    /** Return true iff we have a connection and our expected path exists. */
    public boolean isAlive() {
        boolean             result = true;
        ZooKeeperConnection zkConn = zkRef.get();
        if (zkConn == null || !zkConn.isConnected()) {
            result = false;
        }
        else {
            try {
                final Stat exists = zkConn.exists(fullPath, false);
                if (exists == null) {
                    result = false;
                }
            }
            catch (Exception ex) {
                log.error("failed zk path check", ex);
                result = false;
            }
        }
        log.debug("isAlive(): " + result);
        return result;
    }

    private boolean zkClose(ZooKeeperConnection zkConn) {
        log.debug("zkClose(" + zkConn + ")");
        try {
            if (zkConn != null) {
                zkConn.close();
            }
        } catch (InterruptedException ex) {
            log.error("failed to close ZooKeeper connection", ex);
            return false;
        }
        log.debug("zkClose() succeeded");
        return true;
    }

    public void run() {
        if (isAlive()) return;

        ZooKeeperConnection zkConn = zkRef.getAndSet(null);
        zkClose(zkConn);

        zkConn = new ZooKeeperConnection(zkNodes, SESSION_TIMEOUT_MILLIS);
        try {
            log.info("connecting zkConn: " + zkConn);
            zkConn.connect();
            log.debug("registering watcher: " + this);
            zkConn.register(this);

            log.info("creating rootPath: " + rootPath);
            zkConn.createFullPath(rootPath, new byte[0], CreateMode.PERSISTENT, true);
            log.info("creating fullPath: " + fullPath);
            zkConn.create(fullPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zkRef.set(zkConn);
        }
        catch (Exception ex) {
            log.error("failed to create ZooKeeper state", ex);
        }
    }

    @Override public void close() throws IOException {
        try {
            scheduler.shutdownNow();
        }
        catch (RuntimeException ex) {
            log.error("failed to shutdown scheduler in an orderly fashion", ex);
        }
        ZooKeeperConnection zkConn = zkRef.getAndSet(null);
        zkClose(zkConn);
    }

    @Override public void process(WatchedEvent watchedEvent) {
        log.debug("received watchedEvent" +
                  " type: " + watchedEvent.getType() +
                  " state: " + watchedEvent.getState());

        if (watchedEvent.getType() == Event.EventType.None &&
            watchedEvent.getState() == Event.KeeperState.Expired) {
            log.info("received watchedEvent" +
                     " type: " + watchedEvent.getType() +
                     " state: " + watchedEvent.getState() +
                     " -- kicking zk service thread");

            scheduler.schedule(this, 1, SECONDS);
        }
    }
}
