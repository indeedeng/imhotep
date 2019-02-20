package com.indeed.imhotep;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

/**
 * @author xweng
 */
public interface ImhotepConnectionPool extends Closeable {
    Socket getConnection() throws InterruptedException, IOException;
    boolean releaseConnection(final Socket connection);
}
