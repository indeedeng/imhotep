package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.threads.NamedThreadFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolStatsReporter {
    private final ImhotepConnectionPool connectionPool;

    private final ScheduledExecutorService reportExecutor;

    private final MetricStatsEmitter statsEmitter;

    private final ImhotepConnectionPoolStats stats;

    ImhotepConnectionPoolStatsReporter(
            final ImhotepConnectionPool connectionPool,
            final MetricStatsEmitter statsEmitter) {
        this.connectionPool = connectionPool;
        this.statsEmitter = statsEmitter;
        this.stats = new ImhotepConnectionPoolStats();
        this.reportExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("imhotepConnectionPoolReporter"));
    }

    public void start(final int reportFrequencySeconds) {
        reportExecutor.scheduleAtFixedRate(this::report, reportFrequencySeconds, reportFrequencySeconds, TimeUnit.SECONDS);
    }

    private void report() {
        stats.update(connectionPool.getSourcePool(), connectionPool.getAndResetInvalidatedCount());

        statsEmitter.count("connection.pool.socket.newly.created", stats.getSocketNewlyCreated());
        statsEmitter.count("connection.pool.socket.newly.borrowed", stats.getSocketNewlyBorrowed());
        statsEmitter.count("connection.pool.socket.newly.destroyed", stats.getSocketNewlyDestroyed());
        statsEmitter.count("connection.pool.socket.newly.destroyed.validation", stats.getSocketNewlyDestroyedByValidation());
        statsEmitter.count("connection.pool.socket.newly.invalidated", stats.getSocketInvalidated());
        statsEmitter.count("connection.pool.socket.active", stats.getActiveSocket());
        statsEmitter.count("connection.pool.socket.idle", stats.getIdleSocket());
    }

    private static class ImhotepConnectionPoolStats {
        private long socketNewlyCreated;
        private long socketNewlyBorrowed;
        private long socketNewlyDestroyed;
        private long socketNewlyDestroyedByValidation;
        private long socketInvalidated;
        private long activeSocket;
        private long idleSocket;

        private long lastCreatedCount = 0;
        private long lastBorrowedCount = 0;
        private long lastDestroyedCount = 0;
        private long lastDestroyedByValidationCount = 0;

        public void update(final GenericKeyedObjectPool<Host, Socket> sourcePool, final long invalidatedCount) {
            final long createdCount = sourcePool.getCreatedCount();
            socketNewlyCreated = createdCount - lastCreatedCount;
            lastCreatedCount = createdCount;

            final long borrowedCount = sourcePool.getBorrowedCount();
            socketNewlyBorrowed = borrowedCount - lastBorrowedCount;
            lastBorrowedCount = borrowedCount;

            final long destroyedCount = sourcePool.getDestroyedCount();
            socketNewlyDestroyed = destroyedCount - lastDestroyedCount;
            lastDestroyedCount = destroyedCount;

            final long destroyedByValidationCount = sourcePool.getDestroyedByBorrowValidationCount();
            socketNewlyDestroyedByValidation = destroyedByValidationCount - lastDestroyedByValidationCount;
            lastDestroyedByValidationCount = destroyedByValidationCount;

            socketInvalidated = invalidatedCount;
            activeSocket = sourcePool.getNumActive();
            idleSocket = sourcePool.getNumIdle();
        }

        long getSocketNewlyCreated() {
            return socketNewlyCreated;
        }

        long getSocketNewlyBorrowed() {
            return socketNewlyBorrowed;
        }

        long getSocketNewlyDestroyed() {
            return socketNewlyDestroyed;
        }

        long getSocketNewlyDestroyedByValidation() {
            return socketNewlyDestroyedByValidation;
        }

        long getSocketInvalidated() {
            return socketInvalidated;
        }

        long getActiveSocket() {
            return activeSocket;
        }

        long getIdleSocket() {
            return idleSocket;
        }
    }
}
