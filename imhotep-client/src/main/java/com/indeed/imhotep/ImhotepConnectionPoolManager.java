package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.indeed.imhotep.client.Host;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolManager implements Closeable  {
    private static final Logger log = Logger.getLogger(ImhotepConnectionPoolManager.class);

    private static ImhotepConnectionPoolManager INSTANCE_HOLDER;
    private static final Object singletonLock = new Object();

    public static ImhotepConnectionPoolManager getInstance(final int poolSize) {
        if (INSTANCE_HOLDER == null) {
            synchronized (singletonLock) {
                if (INSTANCE_HOLDER == null) {
                    INSTANCE_HOLDER = new ImhotepConnectionPoolManager(poolSize);
                }
            }
        }
        return INSTANCE_HOLDER;
    }

    private Map<Host, ImhotepConnectionPool> hostConnectionPoolMap;

    private int poolSize;

    private ImhotepConnectionPoolManager(final int poolSize) {
        this.poolSize = poolSize;
        hostConnectionPoolMap = new ConcurrentHashMap<>();
    }

    public @Nullable Socket getConnection(final Host host) {
        ImhotepConnectionPool pool = hostConnectionPoolMap.get(host);
        if (pool == null) {
            // don't want to block other hosts pool, so synchronize host here
            synchronized (host) {
                pool = hostConnectionPoolMap.get(host);
                if (pool == null) {
                    pool = new RemoteImhotepConnectionPool(host, poolSize);
                    hostConnectionPoolMap.put(host, pool);
                }
            }
        }

        try {
            return pool.getConnection();
        } catch (final IOException | InterruptedException e) {
            log.error("can't get connection for " + host, e);
            return null;
        }
    }

    public boolean releaseConnection(final Host host, final Socket socket) {
        final ImhotepConnectionPool pool = hostConnectionPoolMap.get(host);
        return pool.releaseConnection(socket);
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<Host, ImhotepConnectionPool> poolEntry : hostConnectionPoolMap.entrySet()) {
            try {
                poolEntry.getValue().close();
            } catch (final IOException e) {
                log.error("could't close connections for " + poolEntry.getKey(), e);
                Throwables.propagate(e);
            }
        }
    }
}
