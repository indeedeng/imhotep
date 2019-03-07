package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

/**
 * @author xweng
 *
 * recommended usage:
 *
 * testConnnectionPool.withConnection(host, timeoutMillis, connection -> {
 *     // do something with the connection and return a value
 * });
 *
 * or without timeout
 *
 * testConnnectionPool.withConnection(host, connection -> {
 *     // do something with the connection and return a value
 * });
 */
public enum ImhotepConnectionPool implements Closeable {
    INSTANCE;

    private static final Logger logger = Logger.getLogger(ImhotepConnectionPool.class);
    // it's illegal to take the TimeUnit method as an initial value
    private static final long DEFAULT_IDLE_SOCKET_LIVE_MILLIS = 5 * 60 * 1000; // 5 minutes
    // not sure if we need the minimum sockets
    private static final int DEFAULT_MINIMUM_SOCKETS_IN_POOL = 5;

    private final GenericKeyedObjectPool<Host, Socket> sourcePool;

    ImhotepConnectionPool() {
        final ImhotepConnectionKeyedPooledObjectFactory factory = new ImhotepConnectionKeyedPooledObjectFactory();
        final GenericKeyedObjectPoolConfig<Socket> config = new GenericKeyedObjectPoolConfig<>();
        config.setTimeBetweenEvictionRunsMillis(DEFAULT_IDLE_SOCKET_LIVE_MILLIS);
        config.setMaxTotalPerKey(-1);
        config.setMinIdlePerKey(DEFAULT_MINIMUM_SOCKETS_IN_POOL);

        sourcePool = new GenericKeyedObjectPool<>(factory, config);
    }

    /**
     * Get a connection from the pool until connection is returned or errors happen
     */
    private ImhotepConnection getConnection(final Host host) throws IOException {
        try {
            final Socket socket = sourcePool.borrowObject(host);
            return new ImhotepConnection(sourcePool, socket, host);
        } catch (final Exception e) {
            throw Throwables2.propagate(e, IOException.class);
        }
    }

    /**
     * Get a connection from the pool with the timeout in milliseconds
     */
    private ImhotepConnection getConnection(final Host host, final int timeoutMillis) throws IOException {
        try {
            final Socket socket = sourcePool.borrowObject(host, timeoutMillis);
            return new ImhotepConnection(sourcePool, socket, host);
        } catch (final Exception e) {
            throw Throwables2.propagate(e, IOException.class);
        }
    }

    /**
     * Execute the function with connection
     */
    public <R, E extends Exception> R withConnection(
            final Host host,
            final ThrowingFunction<ImhotepConnection, R, E> function) throws E, IOException {
        try (final ImhotepConnection connection = getConnection(host)) {
            try {
                return function.apply(connection);
            } catch (final Throwable t) {
                connection.markAsInvalid();
                throw t;
            }
        }
    }

    /**
     * Execute the function with the connection, also set the timeout when getting connection
     */
    public <R, E extends Exception> R withConnection(
            final Host host,
            final int timeoutMillis,
            final ThrowingFunction<ImhotepConnection, R, E> function) throws E, IOException {
        try (final ImhotepConnection connection = getConnection(host, timeoutMillis)) {
            try {
                return function.apply(connection);
            } catch (final Throwable t) {
                connection.markAsInvalid();
                throw t;
            }
        }
    }

    public interface ThrowingFunction<K, R, E extends Exception> {
        R apply(K k) throws E;
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(sourcePool, logger);
    }
}
