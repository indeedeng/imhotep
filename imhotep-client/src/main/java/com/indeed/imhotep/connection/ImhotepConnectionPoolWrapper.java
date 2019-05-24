package com.indeed.imhotep.connection;

import com.indeed.imhotep.service.MetricStatsEmitter;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolWrapper {
    private static ImhotepConnectionPool INSTANCE;

    public static ImhotepConnectionPool getInstance() {
        return INSTANCE;
    }

    /** It will be called when the daemon start */
    public static synchronized void initializeImhotepConnectionPool(final MetricStatsEmitter statsEmitter) {
        if (INSTANCE == null) {
            INSTANCE = new ImhotepConnectionPool(statsEmitter, new ImhotepConnectionPoolConfig());
        }
    }
}
