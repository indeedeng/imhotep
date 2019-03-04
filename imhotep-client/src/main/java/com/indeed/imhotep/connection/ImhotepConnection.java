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

    private KeyedObjectPool<Host, ImhotepConnection> keyedObjectPool;
    private Socket socket;
    private Host host;

    ImhotepConnection(final KeyedObjectPool keyedObjectPool, final Socket socket, final Host host) {
        this.socket = socket;
        this.host = host;
        this.keyedObjectPool = keyedObjectPool;
    }

    /**
     * Mark a connection as invalid and remove it from the connection pool
     */
    void markAsInvalid() {
        try {
            keyedObjectPool.invalidateObject(host, this);
        } catch (final Exception e) {
            logger.warn("Errors happened when setting connection as invalid, connection is " + this, e);
        }
    }

    public Socket getSocket() {
        return socket;
    }

    @Override
    public String toString() {
        return "ImhotepConnection{" +
                ", socket=" + socket +
                ", host=" + host +
                '}';
    }

    @Override
    public void close() {
        // keyedObjectPool already handles the idempotency
        try {
            keyedObjectPool.returnObject(host, this);
        } catch (final Exception e) {
            logger.warn("Errors happened when returning connection " + this, e);
        }
    }
}
