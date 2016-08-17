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
import org.apache.zookeeper.KeeperException;
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.zookeeper.KeeperException.Code.NODEEXISTS;

/**
   Previous versions of this class suffered from instability due to the
   eccentricities of the ZooKeeper API. So this version takes the simplest
   approach I can think of. We periodically check the ZK connection and the
   existence of our entry in it. If we encounter a problem, we tear down the
   whole shooting match and start over. All direct interaction with ZK happens
   in our single work thread and should thus be serialized. If we receive events
   through the ZK watcher interface, we just kick the work thread and let it
   do its thing.
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

    /**
       Cache the info necessary to populate ZK and start our single work thread.
    */
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

    /**
       Check that we have a connection and that our expected entry exists.

       @return true if all is well
    */
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
        return result;
    }

    /**
       Close our ZK connection.
    */
    private void zkClose(ZooKeeperConnection zkConn) {
        try {
            if (zkConn != null) {
                zkConn.close();
            }
        } catch (InterruptedException ex) {
            log.error("failed to close ZooKeeper connection", ex);
        }
    }

    /**
       Check the status of our ZK connection; tear it down and rebuild it as
       necessary.
     */
    public void run() {
        if (isAlive()) return;

        ZooKeeperConnection zkConn = zkRef.getAndSet(null);
        if (zkConn != null) {
            zkClose(zkConn);
        }

        zkConn = new ZooKeeperConnection(zkNodes, SESSION_TIMEOUT_MILLIS);
        try {
            log.info("connecting zkConn: " + zkConn);
            zkConn.connect();
            zkConn.register(this);

            /* Note that createFullPath() swallows harmless KeeperExceptions */
            log.info("creating rootPath: " + rootPath);
            zkConn.createFullPath(rootPath, new byte[0], CreateMode.PERSISTENT, true);

            log.info("creating fullPath: " + fullPath);
            try {
                zkConn.create(fullPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                              CreateMode.EPHEMERAL);
            }
            catch (KeeperException ex) {
                /* Filter out harmless "node exists" errors. */
                if (ex.code() != NODEEXISTS) {
                    throw ex;
                }
            }

            zkRef.set(zkConn);
        }
        catch (Exception ex) {
            log.error("failed to create ZooKeeper state", ex);
        }
    }

    /**
       Shutdown our work thread and clean up our ZK connection.
    */
    @Override public void close() throws IOException {
        try {
            scheduler.shutdownNow();
        }
        catch (RuntimeException ex) {
            log.error("failed to shutdown scheduler in an orderly fashion", ex);
        }

        ZooKeeperConnection zkConn = zkRef.getAndSet(null);
        if (zkConn != null) {
            zkClose(zkConn);
        }
    }

    /**
       Respond to ZK events that suggest something might be amiss with our
       connection (session timeout, etc.) by kicking our work thread.
     */
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
