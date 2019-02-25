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
    private static final Object singletonLock = new Object();

    public static ImhotepConnectionPoolManager getInstance(final int poolSize) {
        if (instanceHolder == null) {
            synchronized (singletonLock) {
                if (instanceHolder == null) {
                    instanceHolder = new ImhotepConnectionPoolManager(poolSize);
                }
            }
        }
        return instanceHolder;
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

    public void releaseConnection(final Host host, final ImhotepConnection connection) {
        final ImhotepConnectionPool pool = hostConnectionPoolMap.get(host);
        pool.releaseConnection(connection);
    }

    public void discardConnection(final Host host, final ImhotepConnection connection) throws IOException {
        final ImhotepConnectionPool pool = hostConnectionPoolMap.get(host);
        pool.discardConnection(connection);
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(logger, hostConnectionPoolMap.values());
    }
}
