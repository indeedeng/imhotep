package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolTest {

    private static Host host1;
    private static Host host2;

    private static ServerSocket serverSocket1;
    private static ServerSocket serverSocket2;

    private static ImhotepConnectionPool testConnnectionPool;

    @BeforeClass
    public static void initialize() throws IOException {
        serverSocket1 = new ServerSocket(0);
        serverSocket2 = new ServerSocket(0);

        host1 = new Host("localhost", serverSocket1.getLocalPort());
        host2 = new Host("localhost", serverSocket2.getLocalPort());

        testConnnectionPool = ImhotepConnectionPool.INSTANCE;
    }

    @AfterClass
    public static void tearUp() throws IOException {
        serverSocket1.close();
        serverSocket2.close();
        testConnnectionPool.close();
    }

    @Test
    public void testWithConnection() throws IOException {
        final Socket socket1 = testConnnectionPool.withConnection(host1, connection -> {
            final Socket socket = connection.getSocket();
            assertNotNull(socket);
            assertEquals(socket.getInetAddress().getHostName(), host1.getHostname());
            return socket;
        });

        final Socket socket2 = testConnnectionPool.withConnection(host1, connection -> {
            final Socket socket = connection.getSocket();
            assertNotNull(socket);
            assertEquals(socket.getInetAddress().getHostName(), host1.getHostname());
            return socket;
        });

        assertEquals(socket1, socket2);
    }

    @Test
    public void testWithConnectionTimeout() throws IOException {
        try {
            testConnnectionPool.withConnection(new Host("www.google.com", 81), 5000, connection -> {
                final Socket socket = connection.getSocket();
                return socket.getInetAddress().getHostName();
            });
            fail("SocketTimeoutException is expected");
        } catch (final SocketTimeoutException e) {
            // succeed
        }
    }

    @Test
    public void testDiscardConnection() throws IOException {
        Socket socket = null;
        try {
            socket = testConnnectionPool.withConnection(host1, connection -> {
                throw new IllegalArgumentException("a test exception");
            });
        } catch (final IllegalArgumentException e) {
            // do nothing
        }

        final Socket socket2 = testConnnectionPool.withConnection(host1, connection -> connection.getSocket());
        assertNotSame(socket, socket2);
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
        private final int taskIndex;

        Task(final int taskIndex) {
            this.taskIndex = taskIndex;
        }

        @Override
        public Socket call() throws IOException, InterruptedException {
            final Host host = taskIndex % 2 == 0 ? host1 : host2;
            return testConnnectionPool.withConnection(host, connection -> {
                final Socket socket = connection.getSocket();
                Thread.sleep(taskIndex/5 * 100);
                return socket;
            });
        }
    }
}