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
     * Get a singleton connection pool manager
     */
    public static ImhotepConnectionPoolManager getInstance() {
        // add a local holder to avoid reading instanceHolder twice
        ImhotepConnectionPoolManager localHolder = instanceHolder;
        if (localHolder == null) {
            synchronized (singletonMutex) {
                localHolder = instanceHolder;
                if (localHolder == null) {
                    instanceHolder = localHolder = new ImhotepConnectionPoolManager();
                }
            }
        }
        return localHolder;
    }

    private final Map<Host, ImhotepConnectionPool> hostConnectionPoolMap;

    private ImhotepConnectionPoolManager() {
        hostConnectionPoolMap = new ConcurrentHashMap<>();
    }

    public ImhotepConnection getConnection(final Host host) throws IOException {
        final ImhotepConnectionPool pool = hostConnectionPoolMap.computeIfAbsent(
                host,
                missingHost -> new RemoteImhotepConnectionPool(missingHost)
        );
        return pool.getConnection();
    }

    public ImhotepConnection getConnection(final Host host, final int millisecondTimeout) throws IOException  {
        final ImhotepConnectionPool pool = hostConnectionPoolMap.computeIfAbsent(
                host,
                missingHost -> new RemoteImhotepConnectionPool(missingHost)
        );
        return pool.getConnection(millisecondTimeout);
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(logger, hostConnectionPoolMap.values());
    }
}
