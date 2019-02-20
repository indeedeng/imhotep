package com.indeed.imhotep;

import com.indeed.imhotep.client.Host;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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

import static org.junit.Assert.*;

/**
 * @author xweng
 */
public class RemoteImhotepConnectionPoolTest {
    private Host host;

    @Before
    public void initialize() {
        host = new Host("127.0.0.1", 22);
    }

    @Test
    public void testReleaseConnection() throws IOException {
        try(final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2)) {
            final Socket socket = pool.getConnection();
            assertNotNull(socket);
            assertTrue(pool.releaseConnection(socket));
            // duplicated release
            assertFalse(pool.releaseConnection(socket));
        } catch (final InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testGetConnection() throws IOException {
        try(final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2))  {
            final Socket socket = pool.getConnection();
            assertNotNull(socket);
            pool.releaseConnection(socket);

            final Socket socket1 = pool.getConnection();
            assertNotNull(socket1);
            assertEquals(socket1, socket);
            pool.releaseConnection(socket);
        } catch (final InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testGetConnectionConcurrently() throws IOException {
        final Set<Socket> connectionSet = new HashSet<>();
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try (final ImhotepConnectionPool pool = new RemoteImhotepConnectionPool(host, 2)) {
        List<Callable<Socket>> tasks = IntStream.range(0, 16).mapToObj(i -> new Task(pool, i)).collect(Collectors.toList());
            List<Future<Socket>> results = executor.invokeAll(tasks);
            for (Future<Socket> future : results) {
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
            final Socket socket = pool.getConnection();
            assertNotNull(socket);
            assertTrue(socket.isConnected());
            assertTrue(pool.releaseConnection(socket));

            // close the pool
            pool.close();
            assertTrue(socket.isClosed());
        } catch (final InterruptedException e) {
            fail();
        }
    }

    private static class Task implements Callable<Socket>{
        private int taskIndex;
        private ImhotepConnectionPool pool;

        public Task(final ImhotepConnectionPool pool, final int taskIndex) {
            this.pool = pool;
            this.taskIndex = taskIndex;
        }

        @Override
        public Socket call() throws Exception {
            final Socket socket = pool.getConnection();
            Thread.sleep(taskIndex/4*100);
            pool.releaseConnection(socket);
            return socket;
        }
    }
}