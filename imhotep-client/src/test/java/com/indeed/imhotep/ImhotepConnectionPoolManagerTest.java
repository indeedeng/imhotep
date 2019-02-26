package com.indeed.imhotep;

import com.indeed.imhotep.client.Host;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolManagerTest {
    private Host host1;
    private Host host2;

    private ServerSocket serverSocket1;
    private ServerSocket serverSocket2;

    private ImhotepConnectionPoolManager poolManager;

    @Before
    public void initialize() throws IOException {
        final int serverPort1 = 49152;
        final int serverPort2 = 49153;

        host1 = new Host("localhost", serverPort1);
        host2 = new Host("localhost", serverPort2);

        serverSocket1 = new ServerSocket(serverPort1);
        serverSocket2 = new ServerSocket(serverPort2);

        poolManager = ImhotepConnectionPoolManager.getInstance(2);
    }

    @After
    public void finalize() throws IOException {
        serverSocket1.close();
        serverSocket2.close();
        poolManager.close();
    }

    @Test
    public void testReleaseConnection() throws IOException, InterruptedException {
        Socket socket;
        try (final ImhotepConnection connection = poolManager.getConnection(host1)) {
            socket = connection.getSocket();
            assertNotNull(socket);
        }

        final ImhotepConnection connection = poolManager.getConnection(host1);
        socket = connection.getSocket();
        assertNotNull(socket);
        connection.close();
    }

    @Test
    public void testGetConnection() throws IOException, InterruptedException {
        try (final ImhotepConnection connection = poolManager.getConnection(host1)) {
            final Socket socket = connection.getSocket();
            assertNotNull(socket);
            assertEquals(socket.getInetAddress().getHostName(), host1.getHostname());
        }

        try (final ImhotepConnection connection = poolManager.getConnection(host2)) {
            final Socket socket = connection.getSocket();
            assertNotNull(socket);
            assertEquals(socket.getInetAddress().getHostName(), host2.getHostname());
        }
    }

    @Test
    public void testGetConnectionTimeout() throws IOException {
        // socket timeout
        try (final ImhotepConnection connection = poolManager.getConnection(new Host("www.google.com", 81), 1)) {
            fail("SocketTimeoutException is expected");
        } catch (final SocketTimeoutException e) {
            // succeed
        } catch (final InterruptedException | TimeoutException e) {
            fail();
        }

        // concurrent queue timeout
        try (final ImhotepConnection connection = poolManager.getConnection(host1, 1)) {
            final ImhotepConnection connection1 = poolManager.getConnection(host1, 1);
            final ImhotepConnection connection2 = poolManager.getConnection(host1, 1);
            fail("TimeoutException is expected");
        } catch (final TimeoutException e) {
            // succeed
        } catch (final InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testDiscardConnection() throws IOException, InterruptedException {
        Socket socket;
        try (final ImhotepConnection connection = poolManager.getConnection(host1)) {
            socket = connection.getSocket();
            assertNotNull(socket);
            connection.markAsBad();
        }
        assertTrue(socket.isClosed());
    }

    @Test
    public void testGetConnectionConcurrently() {
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
        assertTrue(connectionSet2.size() <= 2 && connectionSet2.size() >= 1);
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
            try (final ImhotepConnection connection = poolManager.getConnection(host)) {
                final Socket socket = connection.getSocket();
                Thread.sleep(taskIndex/5 * 100);
                return socket;
            }
        }
    }
}