package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.net.Socket;

/**
 * @author xweng
 */
public class ImhotepConnection implements Closeable {
    private static final Logger logger = Logger.getLogger(ImhotepConnection.class);

    private final KeyedObjectPool<Host, Socket> sourcePool;
    private final Socket socket;
    private final Host host;
    private boolean closed;

    ImhotepConnection(final KeyedObjectPool<Host, Socket> sourcePool, final Socket socket, final Host host) {
        this.sourcePool = sourcePool;
        this.socket = socket;
        this.host = host;
        this.closed = false;
    }

    /**
     * Mark a connection as invalid and remove it from the connection pool
     */
    void markAsInvalid() {
        try {
            sourcePool.invalidateObject(host, socket);
        } catch (final Exception e) {
            logger.warn("Errors happened when setting socket as invalid, socket is " + socket, e);
        }
    }

    public Socket getSocket() {
        return socket;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;
        try {
            sourcePool.returnObject(host, socket);
        } catch (final Exception e) {
            // do nothing
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
