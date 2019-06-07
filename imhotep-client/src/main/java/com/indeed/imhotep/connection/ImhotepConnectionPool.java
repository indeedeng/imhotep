package com.indeed.imhotep.connection;

import com.google.common.annotations.VisibleForTesting;
import com.indeed.imhotep.client.Host;
import com.indeed.util.core.Pair;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import lombok.experimental.Delegate;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolMXBean;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xweng
 *
 * recommended usage:
 *
 * testConnnectionPool.withBufferedSocketStream(host, timeoutMillis, (in, out) -&gt; {
 *     // do something with the wrapped inputstream and outputstream
 * });
 *
 * testConnnectionPool.withConnection(host, timeoutMillis, connection -&gt; {
 *     // do something with the connection
 * });
 */

public class ImhotepConnectionPool implements Closeable {
    private static final Logger logger = Logger.getLogger(ImhotepConnectionPool.class);

    @Delegate(types = ImhotepConnectionPoolStats.class)
    private final GenericKeyedObjectPool<Host, Socket> sourcePool;

    private final AtomicLong invalidatedConnectionCount;

    ImhotepConnectionPool(final ImhotepConnectionPoolConfig config) {
        final ImhotepConnectionKeyedPooledObjectFactory factory = new ImhotepConnectionKeyedPooledObjectFactory(
                config.getSocketReadTimeoutMills(),
                config.getSocketConnectingTimeoutMills());

        final GenericKeyedObjectPoolConfig<Socket> sourcePoolConfig = new GenericKeyedObjectPoolConfig<>();
        sourcePoolConfig.setMaxIdlePerKey(config.getMaxIdleSocketPerHost());
        sourcePoolConfig.setLifo(true);
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
            return new ImhotepConnection(sourcePool, socket, host);
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
            return new ImhotepConnection(sourcePool, socket, host);
        } catch (final Exception e) {
            throw Throwables2.propagate(e, IOException.class);
        }
    }

    @VisibleForTesting
    GenericKeyedObjectPool<Host, Socket> getSourcePool() {
        return sourcePool;
    }

    long getAndResetInvalidatedCount() {
        return invalidatedConnectionCount.getAndSet(0);
    }

    private void invalidateConnection(final ImhotepConnection connection) {
        invalidatedConnectionCount.incrementAndGet();
        connection.markAsInvalid();
    }

    private Pair<InputStream, OutputStream> getBufferedSocketStream(final ImhotepConnection connection) throws IOException {
        final Socket socket = connection.getSocket();
        return Pair.of(
                new NonClosingInputStream(new BufferedInputStream(socket.getInputStream())),
                new NonClosingOutputStream(new BufferedOutputStream(socket.getOutputStream())));
    }

    /**
     * An execution function to operate with connection.
     */
    public <R, E extends Exception> R withConnection(
            final Host host,
            final ConnectionUser<R, E> function) throws E, IOException {
        try (final ImhotepConnection connection = getConnection(host)) {
            try {
                return function.apply(connection);
            } catch (final Throwable t) {
                invalidateConnection(connection);
                throw t;
            }
        }
    }

    /**
     * An execution function to operate with connection with timeout when getting sockets.
     */
    public <R, E extends Exception> R withConnection(
            final Host host,
            final int timeoutMillis,
            final ConnectionUser<R, E> function) throws E, IOException {
        try (final ImhotepConnection connection = getConnection(host, timeoutMillis)) {
            try {
                return function.apply(connection);
            } catch (final Throwable t) {
                invalidateConnection(connection);
                throw t;
            }
        }
    }

    /**
     * An execution function to read/write to the buffered socket stream.
     * IMPORTANT: You shouldn't return any streams from {@code is} or {@code os} to outside callers in {@code function}.
     * When it reaches to the end of {@code function}, the socket will be returned to the pool automatically.
     * If you really need to return, please take the {@code withConnection} method and close the connection when stream is closed.
     */
    public <R, E extends Exception> R withBufferedSocketStream(
            final Host host,
            final SocketStreamUser<R, E> function) throws E, IOException {
        try (final ImhotepConnection connection = getConnection(host)) {
            final Pair<InputStream, OutputStream> socketStream = getBufferedSocketStream(connection);
            try {
                return function.apply(socketStream.getFirst(), socketStream.getSecond());
            } catch (final Throwable t) {
                invalidateConnection(connection);
                throw t;
            }
        }
    }

    /**
     * An execution function to read/write to the buffered socket stream with timeout when getting connections.
     * IMPORTANT: You shouldn't return any streams from {@code is} or {@code os} to outside callers in {@code function}.
     * When it reaches to the end of {@code function}, the socket will be returned to the pool automatically.
     * If you really need to return, please take the {@code withConnection} method and close the connection when stream is closed.
     */
    public <R, E extends Exception> R withBufferedSocketStream(
            final Host host,
            final int timeoutMillis,
            final SocketStreamUser<R, E> function) throws E, IOException {
        try (final ImhotepConnection connection = getConnection(host, timeoutMillis)) {
            final Pair<InputStream, OutputStream> socketStream = getBufferedSocketStream(connection);
            try {
                return function.apply(socketStream.getFirst(), socketStream.getSecond());
            } catch (final Throwable t) {
                invalidateConnection(connection);
                throw t;
            }
        }
    }

    /**
     * An execution function to read/write to the buffered socket stream, which may throw two kinds of exceptions.
     * IMPORTANT: You shouldn't return any streams from {@code is} or {@code os} to outside callers in {@code function}.
     * When it reaches to the end of {@code function}, the socket will be returned to the pool automatically.
     * If you really need to return, please take the {@code withConnection} method and close the connection when stream is closed.
     */
    public <R, E1 extends Exception, E2 extends Exception> R withBufferedSocketStream2Throwings(
            final Host host,
            final SocketStreamUser2Throwings<R, E1, E2> function) throws E1, E2, IOException {
        try (final ImhotepConnection connection = getConnection(host)) {
            final Pair<InputStream, OutputStream> socketStream = getBufferedSocketStream(connection);
            try {
                return function.apply(socketStream.getFirst(), socketStream.getSecond());
            } catch (final Throwable t) {
                invalidateConnection(connection);
                throw t;
            }
        }
    }

    /**
     * An execution function to read/write to the buffered socket stream with timeout when getting connections. It may throw two kinds of exceptions.
     * IMPORTANT: You shouldn't return any streams from {@code is} or {@code os} to outside callers in {@code function}.
     * When it reaches to the end of {@code function}, the socket will be returned to the pool automatically.
     * If you really need to return, please take the {@code withConnection} method and close the connection when stream is closed.
     */
    public <R, E1 extends Exception, E2 extends Exception> R withBufferedSocketStream2Throwings(
            final Host host,
            final int timeoutMillis,
            final SocketStreamUser2Throwings<R, E1, E2> function) throws E1, E2, IOException {
        try (final ImhotepConnection connection = getConnection(host, timeoutMillis)) {
            final Pair<InputStream, OutputStream> socketStream = getBufferedSocketStream(connection);
            try {
                return function.apply(socketStream.getFirst(), socketStream.getSecond());
            } catch (final Throwable t) {
                invalidateConnection(connection);
                throw t;
            }
        }
    }

    interface ConnectionUser<R, E extends Exception> {
        R apply(ImhotepConnection connection) throws E;
    }

    public interface SocketStreamUser<R, E extends Exception> {
        R apply(InputStream in, OutputStream out) throws E;
    }

    public interface SocketStreamUser2Throwings<R, E1 extends Exception, E2 extends Exception> {
        R apply(InputStream in, OutputStream out) throws E1, E2;
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
