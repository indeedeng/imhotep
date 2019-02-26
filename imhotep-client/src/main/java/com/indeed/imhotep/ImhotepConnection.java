package com.indeed.imhotep;

import java.io.Closeable;
import java.net.Socket;

/**
 * @author xweng
 */
public class ImhotepConnection implements Closeable {
    private ImhotepConnectionPool connectionPool;
    private Socket socket;
    private boolean closed;

    public ImhotepConnection(final ImhotepConnectionPool connectionPool, final Socket socket) {
        this.connectionPool = connectionPool;
        this.socket = socket;
        closed = false;
    }

    public Socket getSocket() {
        return socket;
    }

    public void markAsBad() {
        connectionPool.discardConnection(this);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            connectionPool.releaseConnection(this);
        }
    }
}
