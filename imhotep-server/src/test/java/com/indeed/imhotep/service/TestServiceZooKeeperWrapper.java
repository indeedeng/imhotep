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

import com.indeed.util.io.Files;
import com.indeed.util.zookeeper.*;

import org.apache.log4j.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.quorum.*;

import org.junit.*;
import org.junit.rules.*;

import java.io.*;
import java.util.*;

public class TestServiceZooKeeperWrapper
    implements Watcher {

    private static final int TIMEOUT_SECS   = 3;
    //    private static final int TIMEOUT_MILLIS = 3000;

    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT_SECS * 1000 * 10);

    private final String host    = "localhost";
    private final int    port    = 54321;
    private final String zkNodes = host + ":" + Integer.toString(port);

    private boolean             connected; // !@# should probably be atomic...
    private File                dataDir;
    private ZooKeeperServerMain zkServer;
    private Thread              zkThread;

    @Before
    public void setUp()
        throws IOException, QuorumPeerConfig.ConfigException {

        connected = false;

        dataDir = new File(Files.getTempDirectory("zk-data-dir", Long.toString(System.nanoTime())));
        dataDir.mkdir();

        final Properties props = new Properties();
        props.setProperty("dataDir", dataDir.toString());
        props.setProperty("clientPort", Integer.toString(port));
        //        props.setProperty("tickTime", "1000");

        /*
        Logger zkLogger = Logger.getLogger("org.apache.zookeeper");
        zkLogger.setLevel(Level.TRACE);
        ConsoleAppender appender = new ConsoleAppender(new SimpleLayout(), ConsoleAppender.SYSTEM_ERR);
        zkLogger.addAppender(appender);
        */

        final QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
        quorumConfig.parseProperties(props);

        final ServerConfig config = new ServerConfig();
        config.readFrom(quorumConfig);

        zkServer = new ZooKeeperServerMain();
        zkThread = new Thread() {
                public void run() {
                    try {
                        zkServer.runFromConfig(config);
                    }
                    catch (IOException ex) {
                        System.err.println(ex);
                    }
                }
            };
        zkThread.start();
    }

    @Test
    public void reconnectAfterExpiration()
        throws IOException, InterruptedException, KeeperException {

        final String rootPath = "/" + new Object(){}.getClass().getEnclosingMethod().getName();

        ServiceZooKeeperWrapper wrapper = new ServiceZooKeeperWrapper(zkNodes, host, port, rootPath);

        while (!wrapper.isAlive()) {
            Thread.sleep(100);
        }
    }

    @After
    public void tearDown()
        throws InterruptedException {
        zkThread.interrupt();
        zkThread.join();
        Files.delete(dataDir.toString());
    }

    public void process(WatchedEvent event) {
        System.err.println("Test: " + event);
        if (event.getState() == Event.KeeperState.SyncConnected) {
            synchronized (this) {
                connected = true;
                this.notify();
            }
        }
    }

    private class BogoWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            System.err.println("BogoWatcher: " + event);
        }
    }
}
