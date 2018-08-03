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
 package com.indeed.imhotep.client;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.indeed.util.core.DataLoadingRunnable;
import com.indeed.util.zookeeper.ZooKeeperConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author jsgroth
 */
public class ZkHostsReloader extends DataLoadingRunnable implements HostsReloader {
    private static final Logger log = Logger.getLogger(ZkHostsReloader.class);

    private static final int TIMEOUT = 60000;

    protected final ZooKeeperConnection zkConnection;
    protected final String zkPath;

    protected volatile boolean closed;
    protected volatile List<Host> hosts;

    public ZkHostsReloader(final String zkNodes, final boolean readHostsBeforeReturning) {
        this(zkNodes, "/imhotep/interactive-daemons", readHostsBeforeReturning);
    }

    public ZkHostsReloader(final String zkNodes, final String zkPath, final boolean readHostsBeforeReturning) {
        super("ZkHostsReloader");

        zkConnection = new ZooKeeperConnection(zkNodes, TIMEOUT);
        try {
            zkConnection.connect();
        } catch (final Exception e) {
            throw Throwables.propagate(e);
        }
        this.zkPath = trimEndingSlash(zkPath);
        closed = false;
        hosts = new ArrayList<>();
        if (readHostsBeforeReturning) {
            int retries = 0;
            while (true) {
                updateLastLoadCheck();
                if (load()) {
                    loadComplete();
                    break;
                }
                try {
                    Thread.sleep(TIMEOUT);
                } catch (final InterruptedException e) {
                    log.error("interrupted", e);
                }
                retries++;
                if (retries == 5) {
                    throw new RuntimeException("unable to connect to zookeeper");
                }
            }
        }
    }

    private static String trimEndingSlash(final String s) {
        if (s.endsWith("/")) {
            return s.substring(0, s.length() - 1);
        }
        return s;
    }

    @Override
    public boolean load() {
        if (!closed) {
            try {
                try {
                    try {
                        final List<Host> newHosts = readHostsFromZK();
                        if (!newHosts.equals(hosts)) {
                            hosts = newHosts;
                        }
                        return true;
                    } catch (final KeeperException e) {
                        log.error("zookeeper exception", e);
                        loadFailed();
                        zkConnection.close();
                        zkConnection.connect();
                    }
                } catch (final IOException e) {
                    loadFailed();
                    log.error("io exception", e);
                }
            } catch (final InterruptedException e) {
                log.error("interrupted", e);
                loadFailed();
            }
        }
        return false;
    }

    private List<Host> readHostsFromZK() throws KeeperException, InterruptedException {
        final List<String> childNodes = zkConnection.getChildren(zkPath, false);
        final List<Host> hosts = new ArrayList<>(childNodes.size());
        for (final String childNode : childNodes) {
            final byte[] data = zkConnection.getData(zkPath + "/" + childNode, false, new Stat());
            final String hostString = new String(data, Charsets.UTF_8);
            final String[] splitHostString = hostString.split(":");
            hosts.add(new Host(splitHostString[0], Integer.parseInt(splitHostString[1])));
        }
        Collections.sort(hosts);
        return hosts;
    }

    public List<Host> getHosts() {
        return hosts;
    }

    @Override
    public void shutdown() {
        closed = true;
        try {
            zkConnection.close();
        } catch (final InterruptedException e) {
            log.error(e);
        }
    }
}
