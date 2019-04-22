package com.indeed.imhotep.connection;

import com.google.common.annotations.VisibleForTesting;
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
public enum ImhotepConnectionPool implements Closeable {
    INSTANCE;

    private static final Logger logger = Logger.getLogger(ImhotepConnectionPool.class);

    // We hope the client side time out at first, and then the server socket received EOFException and close it self.
    // The socket time out of server side is 60 seconds, so here we set is as 45 seconds
    private static final int SOCKET_READ_TIMEOUT_MILLIS = 45000;

    // keyedObjectPool doesn't handle the timeout during makeObject, we have to specify it in case of connection block
    private static final int SOCKET_CONNECTING_TIMEOUT_MILLIS = 30000;

    private final GenericKeyedObjectPool<Host, Socket> sourcePool;

    @VisibleForTesting
    static GenericKeyedObjectPool<Host, Socket> makeGenericKeyedObjectPool(final int socketReadTimeoutMills, final int socketConnectingTimeoutMills) {
        final ImhotepConnectionKeyedPooledObjectFactory factory = new ImhotepConnectionKeyedPooledObjectFactory(socketReadTimeoutMills, socketConnectingTimeoutMills);

        final GenericKeyedObjectPoolConfig<Socket> config = new GenericKeyedObjectPoolConfig<>();
        config.setMaxIdlePerKey(16);
        config.setLifo(true);
        config.setTestOnBorrow(true);

        return new GenericKeyedObjectPool<>(factory, config);
    }

    ImhotepConnectionPool() {
        sourcePool = makeGenericKeyedObjectPool(SOCKET_READ_TIMEOUT_MILLIS, SOCKET_CONNECTING_TIMEOUT_MILLIS);
    }

    /**
     * Get a connection from the pool until connection is returned or errors happen
     *
     * An unsafe way to use the connection, which means callers need to close and drop connection by themselves.
     * When callers complete the usage of connection, they should close the connection with {@code ImhotepConnection.close}
     * Whenever there are any {@code Throwable} happening during the usage of connection, callers should mark the connection as invalid by
     * {@code ImhotepConnection.markAsInvalid}
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
