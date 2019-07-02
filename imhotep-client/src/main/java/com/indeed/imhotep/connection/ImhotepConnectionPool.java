package com.indeed.imhotep.connection;

import com.google.common.annotations.VisibleForTesting;
import com.indeed.imhotep.client.Host;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import lombok.experimental.Delegate;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolMXBean;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xweng
 *
 * recommended usage:
 *
 * testConnnectionPool.withConnection(host, timeoutMillis, connection -&gt; {
 *     // do something with the connection and return a value
 * });
 *
 * or without timeout
 *
 * testConnnectionPool.withConnection(host, connection -&gt; {
 *     // do something with the connection and return a value
 * });
 */

public class ImhotepConnectionPool implements Closeable {
    private static final Logger logger = Logger.getLogger(ImhotepConnectionPool.class);

    @Delegate(types = ImhotepConnectionPoolStats.class)
    private final GenericKeyedObjectPool<Host, Socket> sourcePool;

    private final AtomicLong invalidatedConnectionCount;

    ImhotepConnectionPool(final ImhotepConnectionPoolConfig config) {
        final ImhotepConnectionKeyedPooledObjectFactory factory = new ImhotepConnectionKeyedPooledObjectFactory(config);

        final GenericKeyedObjectPoolConfig<Socket> sourcePoolConfig = new GenericKeyedObjectPoolConfig<>();
        // unlimited sockets for every host
        sourcePoolConfig.setMaxTotalPerKey(-1);
        sourcePoolConfig.setMaxTotal(-1);
        sourcePoolConfig.setMaxIdlePerKey(config.getMaxIdleSocketPerHost());
        sourcePoolConfig.setBlockWhenExhausted(false);
        sourcePoolConfig.setLifo(false);
        sourcePoolConfig.setTestOnBorrow(true);

        sourcePool = new GenericKeyedObjectPool<>(factory, sourcePoolConfig);
        invalidatedConnectionCount = new AtomicLong(0);
    }

    /**
     * Get a connection from the pool until connection is returned or errors happen
     *
     * An unsafe way to use the connection, which means callers need to close and drop connection by themselves.
     * When callers complete the usage of connection, they should close the connection with {@code ImhotepConnection.close}
     * Whenever there are any {@code Throwable} happening during the usage of connection, callers should mark the connection as invalid by
     * {@code ImhotepConnection.markAsInvalid}
     * Notice: all bytes caused as a result of this connection must be fully consumed before returning to the pool in case of successful completion
     *
     * @param host the connection host name
     * @return An valid ImhotepConnection
     * @throws IOException
     */
    @VisibleForTesting
    public ImhotepConnection getConnection(final Host host) throws IOException {
        try {
            final Socket socket = sourcePool.borrowObject(host);
            return new ImhotepConnection(this, socket, host);
        } catch (final Exception e) {
            throw Throwables2.propagate(e, IOException.class);
        }
    }

    /**
     * Get a connection from the pool with the timeout in milliseconds
     *
     * An unsafe way to use the connection, which means callers need to close and drop connection by themselves.
     * When callers complete the usage of connection, they should close the connection with {@code ImhotepConnection.close}
     * Whenever there are any {@code Throwable} happening during the usage of connection, callers should mark the connection as invalid by
     * {@code ImhotepConnection.markAsInvalid}
     * Notice: all bytes caused as a result of this connection must be fully consumed before returning to the pool in case of successful completion
     *
     * @param host the connection host name
     * @param timeoutMillis timeout to get the connection
     * @return An valid ImhotepConnection
     * @throws IOException
     */
    public ImhotepConnection getConnection(final Host host, final int timeoutMillis) throws IOException {
        try {
            final Socket socket = sourcePool.borrowObject(host, timeoutMillis);
            return new ImhotepConnection(this, socket, host);
        } catch (final Exception e) {
            throw Throwables2.propagate(e, IOException.class);
        }
    }

    // expose it in the ImhotepConnection
    GenericKeyedObjectPool<Host, Socket> getSourcePool() {
        return sourcePool;
    }

    long getAndResetInvalidatedCount() {
        return invalidatedConnectionCount.getAndSet(0);
    }

    void increaseInvalidatedCount() {
        invalidatedConnectionCount.incrementAndGet();
    }

    /**
     * Execute the function with connection
     */
    public <R, E extends Exception> R withConnection(
            final Host host,
            final ThrowingFunction<ImhotepConnection, R, E> function) throws E, IOException {
        // Note: ImhotepConnection::close don't throw, so the returned value won't be leaked even if R implements closeable.
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
        // Note: ImhotepConnection::close don't throw, so the returned value won't be leaked even if R implements closeable.
        try (final ImhotepConnection connection = getConnection(host, timeoutMillis)) {
            try {
                return function.apply(connection);
            } catch (final Throwable t) {
                connection.markAsInvalid();
                throw t;
            }
        }
    }

    /**
     * Execute the function with the connection and it may throw two kinds of exceptions
     */
    public <R, E1 extends Exception, E2 extends Exception> R withConnectionBinaryException(
            final Host host,
            final BinaryThrowingFunction<ImhotepConnection, R, E1, E2> function) throws E1, E2, IOException {
        // Note: ImhotepConnection::close don't throw, so the returned value won't be leaked even if R implements closeable.
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
     * Execute the function with the connection and it may throw two kinds of exceptions,
     * also set the timeout when getting connection
     */
    public <R, E1 extends Exception, E2 extends Exception> R withConnectionBinaryException(
            final Host host,
            final int timeoutMillis,
            final BinaryThrowingFunction<ImhotepConnection, R, E1, E2> function) throws E1, E2, IOException {
        // Note: ImhotepConnection::close don't throw, so the returned value won't be leaked even if R implements closeable.
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

    public interface BinaryThrowingFunction<K, R, E1 extends Exception, E2 extends Exception> {
        R apply(K k) throws E1, E2;
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(sourcePool, logger);
    }

    private interface ImhotepConnectionPoolStats {
        /**
         * See {@link GenericKeyedObjectPoolMXBean#getBorrowedCount()}
         */
        long getBorrowedCount();

        /**
         * See {@link GenericKeyedObjectPoolMXBean#getCreatedCount()}
         */
        long getCreatedCount();

        /**
         * See {@link GenericKeyedObjectPoolMXBean#getDestroyedCount()}
         */
        long getDestroyedCount();

        /**
         * See {@link GenericKeyedObjectPoolMXBean#getDestroyedByBorrowValidationCount()}
         */
        long getDestroyedByBorrowValidationCount();

        /**
         * See {@link GenericKeyedObjectPoolMXBean#getNumActive()}
         */
        int getNumActive();

        /**
         * See {@link GenericKeyedObjectPoolMXBean#getNumIdle()}
         */
        int getNumIdle();
    }
}
