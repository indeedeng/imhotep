package com.indeed.imhotep;

import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ZkHostsReloader;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author kenh
 */
public class ZkEndpointPersisterTest {
    private TestingServer testingServer;
    private final Closer closer = Closer.create();
    @Before
    public void setUp() throws Exception {
        testingServer = new TestingServer();
    }

    @After
    public void tearDown() throws IOException {
        closer.close();
        testingServer.close();
    }

    static class ClosableReloader implements Closeable {
        private final ZkHostsReloader reloader;

        ClosableReloader(final ZkHostsReloader reloader) {
            this.reloader = reloader;
        }

        @Override
        public void close() throws IOException {
            reloader.shutdown();
        }

        public ZkHostsReloader getReloader() {
            return reloader;
        }
    }

    @Test
    public void testRegisterUnregister() throws InterruptedException, IOException, KeeperException {
        final String zkNodes = testingServer.getConnectString();
        final String zkPath = "/imhotep/daemons";

        try (ZkEndpointPersister daemon1 = new ZkEndpointPersister(zkNodes, zkPath, new Host("DAEMON1", 1230));
             ZkEndpointPersister daemon2 = new ZkEndpointPersister(zkNodes, zkPath, new Host("DAEMON2", 2340))
        ) {
            try (ClosableReloader reloader = new ClosableReloader(new ZkHostsReloader(zkNodes, zkPath, true))) {
                assertEquals(Sets.newHashSet(new Host("DAEMON1", 1230), new Host("DAEMON2", 2340)), Sets.newHashSet(reloader.getReloader().getHosts()));
            }

            try (ZkEndpointPersister daemon3 = new ZkEndpointPersister(zkNodes, zkPath, new Host("DAEMON3", 3450))) {
                try (ClosableReloader reloader = new ClosableReloader(new ZkHostsReloader(zkNodes, zkPath, true))) {
                    assertEquals(Sets.newHashSet(new Host("DAEMON1", 1230), new Host("DAEMON2", 2340), new Host("DAEMON3", 3450)), Sets.newHashSet(reloader.getReloader().getHosts()));
                }

            }

            try (ClosableReloader reloader = new ClosableReloader(new ZkHostsReloader(zkNodes, zkPath, true))) {
                assertEquals(Sets.newHashSet(new Host("DAEMON1", 1230), new Host("DAEMON2", 2340)), Sets.newHashSet(reloader.getReloader().getHosts()));
            }

        }

        try (ClosableReloader reloader = new ClosableReloader(new ZkHostsReloader(zkNodes, zkPath, true))) {
            assertTrue(reloader.getReloader().getHosts().isEmpty());
        }
    }

}