package com.indeed.imhotep;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author xweng
 */
public interface ImhotepConnectionPool extends Closeable {

    ImhotepConnection getConnection() throws InterruptedException, IOException;

    void releaseConnection(final ImhotepConnection connection);

    void discardConnection(final ImhotepConnection connection) throws IOException;

    int getConnectionCount();
}
