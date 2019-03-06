package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

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
 *
 * You can also handle everything by yourself:
 *
 * try (final ImhotepConnection connection = pool.getConnection(host, timeoutMillis)) {
 *     try {
 *         // do something with the connection
 *      } catch (final Throwable t) {
 *         connection.markAsInvalid();
 *         throw t;
 *     }
 * }
 */
public enum ImhotepConnectionPool implements Closeable {
    INSTANCE;

    private static final Logger logger = Logger.getLogger(ImhotepConnectionPool.class);
    // it's illegal to take the TimeUnit method as an initial value
    private static final long DEFAULT_IDLE_SOCKET_LIVE_MILLISECONDS = 5 * 60 * 1000; // 5 minutes
    // not sure if we need the minimum sockets
    private static final int DEFAULT_MINIMUM_SOCKETS_IN_POOL = 5;

    ImhotepConnectionPool() {
        final ImhotepConnectionKeyedPooledObjectFactory factory = new ImhotepConnectionKeyedPooledObjectFactory();
        final GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
        config.setTimeBetweenEvictionRunsMillis(DEFAULT_IDLE_SOCKET_LIVE_MILLISECONDS);
        config.setMaxTotalPerKey(-1);
        config.setMinIdlePerKey(DEFAULT_MINIMUM_SOCKETS_IN_POOL);

        sourcePool = new GenericKeyedObjectPool<Host, ImhotepConnection>(factory, config);
        factory.setKeyedObjectPool(sourcePool);
    }

    private GenericKeyedObjectPool<Host, ImhotepConnection> sourcePool;

    /**
     * Get a connection from the pool until connection is returned or errors happen
     */
    public ImhotepConnection getConnection(final Host host) throws IOException {
        try {
            return sourcePool.borrowObject(host);
        } catch (final Exception e) {
            throw Throwables2.propagate(e, IOException.class);
        }
    }

    /**
     * Get a connection from the pool with the timeout in milliseconds
     */
    public ImhotepConnection getConnection(final Host host, final int timeoutMillis) throws IOException {
        try {
            return sourcePool.borrowObject(host, timeoutMillis);
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
