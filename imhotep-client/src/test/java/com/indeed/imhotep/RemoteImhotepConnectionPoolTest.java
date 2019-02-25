package com.indeed.imhotep;

import com.indeed.imhotep.client.Host;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class RemoteImhotepConnectionPoolTest {
    private Host host;
    private ServerSocket serverSocket;

    @Before
    public void initialize() throws IOException {
        final int serverPort = 49152;
        host = new Host("127.0.0.1", serverPort);
        serverSocket = new ServerSocket(serverPort);
    }

    @After
    public void cleanUp() throws IOException {
        serverSocket.close();
    }

    @Test
    public void testGetConnection() throws IOException {
        try(final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2))  {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);

                final Socket socket1 = connection.getSocket();
                assertNotNull(socket1);
                assertEquals(socket, socket1);
            }
        } catch (final InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testDiscardConnection() throws IOException {
        try(final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                assertEquals(pool.getConnectionCount(), 1);
                // something wrong happened and discard the connection
                pool.discardConnection(connection);
            }
            assertEquals(pool.getConnectionCount(), 0);
        } catch (final InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testGetConnectionConcurrently() throws IOException {
        final Set<Socket> connectionSet = new HashSet<>();
        final ExecutorService executor = Executors.newFixedThreadPool(16);
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2)) {
            List<Callable<Socket>> tasks = IntStream.range(0, 16).mapToObj(i -> new Task(pool, i)).collect(Collectors.toList());
            List<Future<Socket>> results = executor.invokeAll(tasks);
            for (final Future<Socket> future : results) {
                final Socket socket = future.get();
                if (socket == null) {
                    fail();
                }
                connectionSet.add(socket);
            }
        } catch (final InterruptedException | ExecutionException e) {
            fail();
        }

        assertTrue(connectionSet.size() <= 2);
    }

    @Test
    public void testClose() throws IOException {
        try(final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                assertFalse(socket.isClosed());

                // close the pool
                pool.close();
                assertTrue(socket.isClosed());
            }
        } catch (final InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testIncorrectReleaseConnection() throws IOException {
        // duplicated release
        try(final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                // first release
                pool.releaseConnection(connection);
            }
            assertEquals(pool.getConnectionCount(), 1);
        } catch (final InterruptedException e) {
            fail();
        }

        // forget to release
        Socket socket = null;
        try(final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2)) {
            final ImhotepConnection connection = pool.getConnection();
            socket = connection.getSocket();
            assertNotNull(socket);
        } catch (final InterruptedException e) {
            fail();
        }
        assertTrue(socket.isClosed());
    }

    @Test
    public void testIncorrectDiscardConnection() throws IOException {
        // discard + release
        try(final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                assertEquals(pool.getConnectionCount(), 1);
                pool.discardConnection(connection);
            }
            assertEquals(pool.getConnectionCount(), 0);
        } catch (final InterruptedException e) {
            fail();
        }

        // discard + release
        try(final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                assertEquals(pool.getConnectionCount(), 1);
            }
            // discard a connection not belonging the pool
            final ImhotepConnection otherConnection = new ImhotepConnection(pool, new Socket(host.getHostname(), host.getPort()));
            pool.discardConnection(otherConnection);
            assertEquals(pool.getConnectionCount(), 1);
        } catch (final InterruptedException e) {
            fail();
        }
    }

    private static class Task implements Callable<Socket>{
        private final int taskIndex;
        private final ImhotepConnectionPool pool;

        public Task(final ImhotepConnectionPool pool, final int taskIndex) {
            this.pool = pool;
            this.taskIndex = taskIndex;
        }

        @Override
        public Socket call() throws Exception {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                Thread.sleep(taskIndex/4*100);
                return socket;
            }
        }
    }
}