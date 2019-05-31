package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolTest {

    private Host host1;
    private Host host2;

    private ServerSocket serverSocket1;
    private ServerSocket serverSocket2;

    private ImhotepConnectionPool testConnnectionPool;

    @Before
    public void initialize() throws IOException {
        serverSocket1 = new ServerSocket(0);
        serverSocket2 = new ServerSocket(0);

        host1 = new Host("localhost", serverSocket1.getLocalPort());
        host2 = new Host("localhost", serverSocket2.getLocalPort());

        final ImhotepConnectionPoolConfig config = new ImhotepConnectionPoolConfig();
        config.setSocketReadTimeoutMills(100);
        config.setSocketConnectingTimeoutMills(1000);
        config.setMaxIdleSocketPerHost(8);
        testConnnectionPool = new ImhotepConnectionPool(config);
    }

    @After
    public void tearUp() throws IOException {
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

    @Test(expected = SocketTimeoutException.class)
    public void testWithConnectionTimeout() throws IOException {
        testConnnectionPool.withConnection(new Host("www.google.com", 81), 5000, connection -> {
            final Socket socket = connection.getSocket();
            return socket.getInetAddress().getHostName();
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDiscardConnection() throws IOException {
        testConnnectionPool.withConnection(host1, connection -> {
            throw new IllegalArgumentException("a test exception");
        });
    }

    @Test
    public void testInvalidateTimeoutSocket() throws Exception {
        final Socket firstSocket = testConnnectionPool.getConnection(host1).getSocket();
        Thread.sleep(200);

        final Socket secondSocket = testConnnectionPool.getConnection(host1).getSocket();
        assertFalse(Objects.equals(firstSocket, secondSocket));
    }

    /** As methods close and markAsInvalid swallow exceptions happening inside with logging.
     * Here following 4 tests manually close/markAsInvalid to ensure they work well */
    @Test(expected = NullPointerException.class)
    public void testDiscardConnectionIncorrectly() throws Exception {
        final ImhotepConnection connection = testConnnectionPool.getConnection(host1);
        final GenericKeyedObjectPool<Host, Socket> sourcePool = testConnnectionPool.getSourcePool();

        sourcePool.invalidateObject(host1, connection.getSocket());
        sourcePool.returnObject(host1, connection.getSocket());
    }

    @Test
    public void testDiscardConnectionCorrectly() throws Exception {
        final ImhotepConnection connection = testConnnectionPool.getConnection(host1);
        final GenericKeyedObjectPool<Host, Socket> sourcePool = testConnnectionPool.getSourcePool();

        sourcePool.invalidateObject(host1, connection.getSocket());
    }

    @Test
    public void testReturnConnectionCorrectly() throws Exception {
        final ImhotepConnection connection = testConnnectionPool.getConnection(host1);
        final GenericKeyedObjectPool<Host, Socket> sourcePool = testConnnectionPool.getSourcePool();

        sourcePool.returnObject(host1, connection.getSocket());
    }

    @Test
    public void testReturnConnectionIncorrectly() throws Exception {
        final ImhotepConnection connection = testConnnectionPool.getConnection(host1);
        final GenericKeyedObjectPool<Host, Socket> sourcePool = testConnnectionPool.getSourcePool();

        sourcePool.returnObject(host1, connection.getSocket());
        sourcePool.invalidateObject(host1, connection.getSocket());
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