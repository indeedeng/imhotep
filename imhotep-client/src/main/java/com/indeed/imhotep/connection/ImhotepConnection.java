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

    private KeyedObjectPool<Host, ImhotepConnection> sourcePool;
    private Socket socket;
    private Host host;

    ImhotepConnection(final KeyedObjectPool sourcePool, final Socket socket, final Host host) {
        this.sourcePool = sourcePool;
        this.socket = socket;
        this.host = host;
    }

    /**
     * Mark a connection as invalid and remove it from the connection pool
     */
    public void markAsInvalid() {
        try {
            sourcePool.invalidateObject(host, this);
        } catch (final Exception e) {
            logger.warn("Errors happened when setting connection as invalid, connection is " + this, e);
        }
    }

    public Socket getSocket() {
        return socket;
    }

    @Override
    public void close() {
        // KeyedObjectPool already handles the idempotency
        try {
            sourcePool.returnObject(host, this);
        } catch (final Exception e) {
            logger.warn("Errors happened when returning connection " + this, e);
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
