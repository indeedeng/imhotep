package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author xweng
 */
public class ImhotepConnection implements Closeable {
    private static final Logger logger = Logger.getLogger(ImhotepConnection.class);

    private final KeyedObjectPool<Host, Socket> sourcePool;
    private final ImhotepConnectionPool connectionPool;
    private final Socket socket;
    private final Host host;
    private final AtomicBoolean closedOrInvalidated;

    ImhotepConnection(final ImhotepConnectionPool connectionPool, final Socket socket, final Host host) {
        this.connectionPool = connectionPool;
        this.sourcePool = connectionPool.getSourcePool();
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

        // update the invalidated count
        connectionPool.increaseInvalidatedCount();
        try {
            sourcePool.invalidateObject(host, socket);
        } catch (final Throwable e) {
            logger.warn("Errors happened when setting socket as invalid, socket is " + socket, e);
        }
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

        try {
            sourcePool.returnObject(host, socket);
        } catch (final Throwable e) {
            logger.warn("Errors happened when returning socket, socket = " + socket, e);
        }
    }

    @Override
    public String toString() {
        return "ImhotepConnection{" +
                "socket=" + socket +
                ", host=" + host +
                '}';
    }
}
