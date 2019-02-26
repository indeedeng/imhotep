package com.indeed.imhotep;

import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author xweng
 */
public interface ImhotepConnectionPool extends Closeable {

    /**
     * Get a connection from the pool until connection is returned or errors happen
     */
    ImhotepConnection getConnection() throws InterruptedException, IOException;

    /**
     * Get a connection from the pool with the timeout in milliseconds
     * Throw SocketTimeoutException if it's timeout when connecting sockets
     * Throw TimeoutException if it's timeout waiting for dequeue
     * @param millisecondTimeout
     */
    ImhotepConnection getConnection(final int millisecondTimeout) throws InterruptedException, IOException, TimeoutException;

    /**
     * Release the connection and restore it to the pool
     */
    void releaseConnection(final ImhotepConnection connection);

    /**
     * Discard a bad connection and remove it from the pool
     * @param connection
     */
    void discardConnection(final ImhotepConnection connection);

    @VisibleForTesting
    int getConnectionCount();
}
