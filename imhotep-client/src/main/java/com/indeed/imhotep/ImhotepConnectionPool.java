package com.indeed.imhotep;

import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author xweng
 */
public interface ImhotepConnectionPool extends Closeable {

    /**
     * Get a connection from the pool until connection is returned or errors happen
     */
    ImhotepConnection getConnection() throws IOException;

    /**
     * Get a connection from the pool with the timeout in milliseconds
     */
    ImhotepConnection getConnection(final int millisecondTimeout) throws IOException;

    /**
     * Release the connection and restore it to the pool
     */
    void releaseConnection(final ImhotepConnection connection);

    /**
     * Discard a bad connection and remove it from the pool
     * @param connection
     */
    void discardConnection(final ImhotepConnection connection);

    /**
     * Get total number of connections (available + occupied)
     */
    @VisibleForTesting
    int getConnectionCount();
}
