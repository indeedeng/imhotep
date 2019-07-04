package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;

import java.io.Closeable;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author xweng
 */
public class ImhotepConnection implements Closeable {
    private final ImhotepConnectionPool connectionPool;
    private final Socket socket;
    private final Host host;
    private final AtomicBoolean closedOrInvalidated;

    ImhotepConnection(final ImhotepConnectionPool connectionPool, final Socket socket, final Host host) {
        this.connectionPool = connectionPool;
        this.socket = socket;
        this.host = host;
        this.closedOrInvalidated = new AtomicBoolean(false);
    }

    /**
     * Mark a connection as invalid and remove it from the connection pool
     */
    public void markAsInvalid() {
        if (closedOrInvalidated.getAndSet(true)) {
            return;
        }
        connectionPool.invalidSocket(host, socket);
    }

    public Socket getSocket() {
        return socket;
    }

    /**
     * This close method don't throw.
     * This fact is used in {@link ImhotepConnectionPool#withConnection} family.
     */
    @Override
    public void close() {
        if (closedOrInvalidated.getAndSet(true)) {
            return;
        }
        connectionPool.returnSocket(host, socket);
    }

    @Override
    public String toString() {
        return "ImhotepConnection{" +
                "socket=" + socket +
                ", host=" + host +
                '}';
    }
}
