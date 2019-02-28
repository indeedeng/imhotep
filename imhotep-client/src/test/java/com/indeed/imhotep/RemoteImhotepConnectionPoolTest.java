package com.indeed.imhotep;

import com.indeed.imhotep.client.Host;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
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
        serverSocket = new ServerSocket(0);
        host = new Host("localhost", serverSocket.getLocalPort());
    }

    @After
    public void cleanUp() throws IOException {
        serverSocket.close();
    }

    @Test
    public void testGetConnection() throws IOException {
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);

                final Socket socket1 = connection.getSocket();
                assertNotNull(socket1);
                assertEquals(socket, socket1);
            }
        } catch (final IOException e) {
            fail();
        }
    }

    @Test
    public void testGetConnectionTimeout() {
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(new Host("www.google.com", 81))) {
            try (final ImhotepConnection connection = pool.getConnection(1)) {
            }
            fail("SocketTimeoutException is expected");
        } catch (final SocketTimeoutException e) {
            // succeed
        } catch (final IOException e) {
            fail();
        }
    }

    @Test
    public void testDiscardConnection() {
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                assertEquals(pool.getConnectionCount(), 1);
                // something wrong happened and discard the connection
                pool.discardConnection(connection);
            }
            assertEquals(pool.getConnectionCount(), 0);
        } catch (final IOException e) {
            fail();
        }
    }

    @Test
    public void testGetConnectionConcurrently() throws IOException {
        final ExecutorService executor = Executors.newFixedThreadPool(16);
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host)) {
            List<Callable<Socket>> tasks = IntStream.range(0, 16).mapToObj(i -> new Task(pool, i)).collect(Collectors.toList());
            List<Future<Socket>> results = executor.invokeAll(tasks);
            for (final Future<Socket> future : results) {
                final Socket socket = future.get();
                if (socket == null) {
                    fail();
                }
            }
        } catch (final InterruptedException | ExecutionException e) {
            fail();
        }
    }

    @Test
    public void testCleanUpSocket() throws InterruptedException, IOException {
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 10)) {
            final ImhotepConnection connection1 = pool.getConnection();
            final ImhotepConnection connection2 = pool.getConnection();
            pool.releaseConnection(connection1);
            pool.releaseConnection(connection2);

            assertEquals(pool.getConnectionCount(), 2);

            Thread.sleep(20);
            pool.getConnection();
            assertEquals(1, pool.getConnectionCount());
        }
    }

    @Test
    public void testClose() {
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                assertFalse(socket.isClosed());

                // close the pool
                pool.close();
                assertTrue(socket.isClosed());
            }
        } catch (final IOException e) {
            fail();
        }
    }

    @Test
    public void testIncorrectReleaseConnection() throws IOException {
        // duplicated release
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                // first release
                pool.releaseConnection(connection);
            }
            assertEquals(pool.getConnectionCount(), 1);
        } catch (final IOException e) {
            fail();
        }

        // forget to release
        Socket socket = null;
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host)) {
            final ImhotepConnection connection = pool.getConnection();
            socket = connection.getSocket();
            assertNotNull(socket);
        } catch (final IOException e) {
            fail();
        }
        assertTrue(socket.isClosed());
    }

    @Test
    public void testIncorrectDiscardConnection() throws IOException {
        // discard + release
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                assertEquals(pool.getConnectionCount(), 1);
                pool.discardConnection(connection);
            }
            assertEquals(pool.getConnectionCount(), 0);
        } catch (final IOException e) {
            fail();
        }

        // discard a connection not from the pool
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host)) {
            try (final ImhotepConnection connection = pool.getConnection()) {
                final Socket socket = connection.getSocket();
                assertNotNull(socket);
                assertEquals(pool.getConnectionCount(), 1);
            }
            // discard a connection not belonging the pool
            final ImhotepConnection otherConnection = new ImhotepConnection(pool, new Socket(host.getHostname(), host.getPort()));
            pool.discardConnection(otherConnection);
            assertEquals(pool.getConnectionCount(), 1);
        } catch (final IOException e) {
            fail();
        }
    }

    private static class Task implements Callable<Socket> {
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
                Thread.sleep(taskIndex / 4 * 100);
                return socket;
            }
        }
    }
}