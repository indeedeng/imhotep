package com.indeed.imhotep;

import com.indeed.imhotep.client.Host;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolManager implements Closeable  {
    private static final Logger logger = Logger.getLogger(ImhotepConnectionPoolManager.class);

    private static volatile ImhotepConnectionPoolManager instanceHolder;
    private static final Object singletonMutex = new Object();

    /**
     * Get a singleton connection pool manager with fixed size
     * @param maxPoolSize The specified pool size
     */
    public static ImhotepConnectionPoolManager getInstance(final int maxPoolSize) {
        // add a local holder to avoid reading instanceHolder twice
        ImhotepConnectionPoolManager localHolder = instanceHolder;
        if (localHolder == null) {
            synchronized (singletonMutex) {
                localHolder = instanceHolder;
                if (localHolder == null) {
                    instanceHolder = localHolder = new ImhotepConnectionPoolManager(maxPoolSize);
                }
            }
        }

        if (localHolder.getMaxPoolSize() != maxPoolSize) {
            throw new IllegalArgumentException("Already have a connection pool with size " + localHolder.getMaxPoolSize());
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

    private final int maxPoolSize;

    private ImhotepConnectionPoolManager(final int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        hostConnectionPoolMap = new ConcurrentHashMap<>();
    }

    public ImhotepConnection getConnection(final Host host) throws IOException, InterruptedException {
        final ImhotepConnectionPool pool = hostConnectionPoolMap.computeIfAbsent(
                host,
                missingHost -> new RemoteImhotepConnectionPool(missingHost, maxPoolSize)
        );
        return pool.getConnection();
    }

    public ImhotepConnection getConnection(final Host host, final int millisecondTimeout) throws IOException, InterruptedException, TimeoutException {
        final ImhotepConnectionPool pool = hostConnectionPoolMap.computeIfAbsent(
                host,
                missingHost -> new RemoteImhotepConnectionPool(missingHost, maxPoolSize)
        );
        return pool.getConnection(millisecondTimeout);
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(logger, hostConnectionPoolMap.values());
    }
}
