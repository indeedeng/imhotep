package com.indeed.imhotep;

import com.indeed.imhotep.client.Host;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolManager implements Closeable  {
    private static final Logger logger = Logger.getLogger(ImhotepConnectionPoolManager.class);

    private static volatile ImhotepConnectionPoolManager instanceHolder;
    private static final Object singletonMutex = new Object();

    /**
     * Get a singleton connection pool manager with fixed size
     * @param poolSize The specified pool size
     */
    public static ImhotepConnectionPoolManager getInstance(final int poolSize) {
        // add a local holder to avoid reading instanceHolder twice
        ImhotepConnectionPoolManager localHolder = instanceHolder;
        if (localHolder == null) {
            synchronized (singletonMutex) {
                localHolder = instanceHolder;
                if (localHolder == null) {
                    instanceHolder = localHolder = new ImhotepConnectionPoolManager(poolSize);
                }
            }
        }

        if (localHolder.getPoolSize() != poolSize) {
            throw new IllegalArgumentException("Already have a connection pool with size " + localHolder.getPoolSize());
        }
        return localHolder;
    }

    /**
     * Get a singleton connection pool manager with unlimited size
     */
    public static ImhotepConnectionPoolManager getInstance() {
        return getInstance(Integer.MAX_VALUE);
    }

    private final Map<Host, ImhotepConnectionPool> hostConnectionPoolMap;

    private final int poolSize;

    private ImhotepConnectionPoolManager(final int poolSize) {
        this.poolSize = poolSize;
        hostConnectionPoolMap = new ConcurrentHashMap<>();
    }

    public ImhotepConnection getConnection(final Host host) throws IOException, InterruptedException {
        final ImhotepConnectionPool pool = hostConnectionPoolMap.computeIfAbsent(
                host,
                missingHost -> new RemoteImhotepConnectionPool(missingHost, poolSize)
        );
        return pool.getConnection();
    }

    public int getPoolSize() {
        return poolSize;
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(logger, hostConnectionPoolMap.values());
    }
}
