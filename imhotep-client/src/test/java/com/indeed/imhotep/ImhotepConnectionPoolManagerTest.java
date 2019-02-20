package com.indeed.imhotep;

import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.client.Host;
import com.sun.deploy.util.SystemUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolManagerTest {

    private Host host1;
    private Host host2;
    private Host host3;

    private ImhotepConnectionPoolManager poolManager;

    @Before
    public void initialize() {
        host1 = new Host("localhost", 22);
        host2 = new Host("localhost", 80);
        host3 = new Host("localhost", 8081);
        poolManager = ImhotepConnectionPoolManager.getInstance(2);
    }

    @After
    public void finalize() throws IOException {
        poolManager.close();
    }

    @Test
    public void testReleaseConnection() throws IOException {
        final Socket socket1 = poolManager.getConnection(host1);
        assertNotNull(socket1);
        assertTrue(poolManager.releaseConnection(host1, socket1));
        assertFalse(poolManager.releaseConnection(host1, socket1));
    }

    @Test
    public void testGetConnection() throws IOException {
        final Socket socket1 = poolManager.getConnection(host1);
        assertNotNull(socket1);
        assertEquals(socket1.getInetAddress().getHostName(), host1.getHostname());
        assertTrue(poolManager.releaseConnection(host1, socket1));

        final Socket socket2 = poolManager.getConnection(host2);
        assertNotNull(socket2);
        assertEquals(socket2.getInetAddress().getHostName(), host2.getHostname());
        assertTrue(poolManager.releaseConnection(host2, socket2));
    }

    @Test
    public void testGetConnectionConcurrently() throws IOException {
        final Set<Socket> connectionSet1 = new HashSet<>();
        final Set<Socket> connectionSet2 = new HashSet<>();

        ExecutorService executor = Executors.newFixedThreadPool(32);
        List<Callable<Socket>> tasks = IntStream.range(0, 32).mapToObj(i -> new Task(i)).collect(Collectors.toList());

        try {
            List<Future<Socket>> results = executor.invokeAll(tasks);
            for (Future<Socket> future : results) {
                final Socket socket = future.get();
                if (socket == null) {
                    fail();
                }

                if (socket.getPort() == host1.port) {
                    connectionSet1.add(socket);
                } else {
                    connectionSet2.add(socket);
                }
            }
        } catch (final InterruptedException | ExecutionException e) {
            fail();
        }

        assertTrue(connectionSet1.size() <= 2 && connectionSet1.size() >= 1);
        assertTrue(connectionSet2.size() <= 2 && connectionSet1.size() >= 1);
        for (final Socket socket : connectionSet1) {
            assertEquals(socket.getInetAddress().getHostName(), host1.getHostname());
            assertEquals(socket.getPort(), host1.getPort());
        }

        for (final Socket socket : connectionSet2) {
            assertEquals(socket.getInetAddress().getHostName(), host2.getHostname());
            assertEquals(socket.getPort(), host2.getPort());
        }
    }

    private class Task implements Callable<Socket>{
        private int taskIndex;

        public Task(final int taskIndex) {
            this.taskIndex = taskIndex;
        }

        @Override
        public Socket call() throws Exception {
            final Host host = taskIndex % 2 == 0 ? host1 : host2;
            final Socket socket = poolManager.getConnection(host);
            Thread.sleep(taskIndex/5 * 100);
            poolManager.releaseConnection(host, socket);
            return socket;
        }
    }
}