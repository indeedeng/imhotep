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

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

/**
 * @author jsgroth
 */
public class ServiceZooKeeperWrapper implements Closeable {
    private static final Logger log = Logger.getLogger(ServiceZooKeeperWrapper.class);

    private static final int TIMEOUT = 60000;

    private final ZooKeeperConnection zkConnection;
    private final String rootPath, nodeName;
    private final byte[] data;
    private volatile boolean closed;

    public ServiceZooKeeperWrapper(final String zkNodes, final String hostname, final int port,
                                   final String rootPath) {
        zkConnection = new ZooKeeperConnection(zkNodes, TIMEOUT);
        this.rootPath = rootPath;
        nodeName = UUID.randomUUID().toString();
        data = (hostname+":"+port).getBytes(Charsets.UTF_8);
        closed = false;

        createNode();
        zkConnection.register(new MyWatcher());
    }

    @Override
    public void close() throws IOException {
        closed = true;
        try {
            zkConnection.close();
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    public boolean isAlive() {
        return zkConnection.getState().isAlive();
    }

    private void createNode() {
        boolean created = false;
        while (!created) {
            try {
                try {
                    zkConnection.connect();
                    ZooKeeperConnection.createFullPath(zkConnection, rootPath, new byte[0], CreateMode.PERSISTENT, true);
                    zkConnection.create(rootPath + "/"+nodeName, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    created = true;
                } catch (IOException e) {
                    log.error(e);
                    Thread.sleep(TIMEOUT);
                } catch (KeeperException e) {
                    log.error(e);
                    zkConnection.close();
                    Thread.sleep(TIMEOUT);
                }
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
    }

    private class MyWatcher implements Watcher {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (!closed) {
                if (watchedEvent.getType() == Event.EventType.None && watchedEvent.getState() != Event.KeeperState.SyncConnected) {
                    try {
                        zkConnection.close();
                    } catch (InterruptedException e) {
                        log.error(e);
                    }
                    createNode();
                }
                zkConnection.register(this);
            }
        }
    }
}
