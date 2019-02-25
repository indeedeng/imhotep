package com.indeed.imhotep;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

/**
 * @author xweng
 */
public class ImhotepConnection implements Closeable {
    private ImhotepConnectionPool connectionPool;
    private Socket socket;

    public ImhotepConnection(final ImhotepConnectionPool connectionPool, final Socket socket) {
        this.connectionPool = connectionPool;
        this.socket = socket;
    }

    public Socket getSocket() {
        return socket;
    }

    @Override
    public void close() throws IOException {
        connectionPool.releaseConnection(this);
    }
}
