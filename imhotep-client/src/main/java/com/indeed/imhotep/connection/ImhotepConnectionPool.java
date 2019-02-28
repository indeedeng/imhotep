package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;
import com.indeed.util.core.io.Closeables2;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author xweng
 */
public class ImhotepConnectionPool implements Closeable {
    private static final Logger logger = Logger.getLogger(ImhotepConnectionPool.class);

    private static final long DEFAULT_IDLE_SOCKET_LIVE_MILLISECONDS = TimeUnit.MINUTES.toMillis(5);
    // not sure if we need the minimum sockets
    private static final int DEFAULT_MINIMUM_SOCKETS_IN_POOL = 5;

    private static volatile ImhotepConnectionPool instanceHolder;

    private static final Object singletonMutex = new Object();

    /**
     * Get a singleton imhotep connection pool
     */
    public static ImhotepConnectionPool getInstance() {
        // add a local holder to avoid reading instanceHolder twice
        ImhotepConnectionPool localHolder = instanceHolder;
        if (localHolder == null) {
            synchronized (singletonMutex) {
                localHolder = instanceHolder;
                if (localHolder == null) {
                    instanceHolder = localHolder = new ImhotepConnectionPool();
                }
            }
        }
        return localHolder;
    }

    private GenericKeyedObjectPool<Host, ImhotepConnection> keyedObjectPool;

    private ImhotepConnectionPool() {
        final KeyedPooledObjectFactory factory = new ImhotepConnectionKeyedPooledObjectFactory();
        final GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
        config.setTimeBetweenEvictionRunsMillis(DEFAULT_IDLE_SOCKET_LIVE_MILLISECONDS);
        config.setMaxTotalPerKey(-1);
        config.setMinIdlePerKey(DEFAULT_MINIMUM_SOCKETS_IN_POOL);

        keyedObjectPool = new GenericKeyedObjectPool<Host, ImhotepConnection>(factory, config);
    }

    /**
     * Get a connection from the pool until connection is returned or errors happen
     */
    public ImhotepConnection getConnection(final Host host) throws Exception {
        final ImhotepConnection connection = keyedObjectPool.borrowObject(host);
        connection.setKeyedObjectPool(keyedObjectPool);
        return connection;
    }

    /**
     * Get a connection from the pool with the timeout in milliseconds
     */
    public ImhotepConnection getConnection(final Host host, final int millisecondTimeout) throws Exception {
        final ImhotepConnection connection = keyedObjectPool.borrowObject(host, millisecondTimeout);
        connection.setKeyedObjectPool(keyedObjectPool);
        return connection;
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeQuietly(keyedObjectPool, logger);
    }
}
