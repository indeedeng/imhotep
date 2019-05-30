package com.indeed.imhotep.connection;

import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.threads.NamedThreadFactory;

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

    public ImhotepConnectionPoolStatsReporter(
            final ImhotepConnectionPool connectionPool,
            final MetricStatsEmitter statsEmitter) {
        this.connectionPool = connectionPool;
        this.statsEmitter = statsEmitter;
        this.stats = new ImhotepConnectionPoolStats();
        this.reportExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("imhotepConnectionPoolReporter"));
    }

    public void start(final int reportFrequencySeconds) {
        stats.update(connectionPool);
        reportExecutor.scheduleAtFixedRate(this::report, reportFrequencySeconds, reportFrequencySeconds, TimeUnit.SECONDS);
    }

    private void report() {
        stats.update(connectionPool);

        // the count of socket created recently
        statsEmitter.count("connection.pool.socket.created", stats.getSocketCreated());
        // the count of socket borrowed recently
        statsEmitter.count("connection.pool.socket.borrowed", stats.getSocketBorrowed());
        // the count of socket destroyed recently
        statsEmitter.count("connection.pool.socket.destroyed", stats.getSocketDestroyed());
        // the count of sockets failing to pass the validate check when borrowing
        statsEmitter.count("connection.pool.socket.failed.validation", stats.getSocketFailedValidation());
        // the count of sockets invalidated manually when exceptions happen with that socket
        statsEmitter.count("connection.pool.socket.invalidated", stats.getSocketInvalidated());
        // active sockets count in the pool
        statsEmitter.gauge("connection.pool.socket.active", stats.getActiveSocket());
        // idle sockets count in the pool
        statsEmitter.gauge("connection.pool.socket.idle", stats.getIdleSocket());
    }

    private static class ImhotepConnectionPoolStats {
        private long socketCreated;
        private long socketBorrowed;
        private long socketDestroyed;
        private long socketFailedValidation;
        private long socketInvalidated;
        private long activeSocket;
        private long idleSocket;

        private long lastCreatedCount = 0;
        private long lastBorrowedCount = 0;
        private long lastDestroyedCount = 0;
        private long lastDestroyedByValidationCount = 0;

        public void update(final ImhotepConnectionPool connectionPool) {
            final long createdCount = connectionPool.getCreatedCount();
            socketCreated = createdCount - lastCreatedCount;
            lastCreatedCount = createdCount;

            final long borrowedCount = connectionPool.getBorrowedCount();
            socketBorrowed = borrowedCount - lastBorrowedCount;
            lastBorrowedCount = borrowedCount;

            final long destroyedCount = connectionPool.getDestroyedCount();
            socketDestroyed = destroyedCount - lastDestroyedCount;
            lastDestroyedCount = destroyedCount;

            final long destroyedByValidationCount = connectionPool.getDestroyedByBorrowValidationCount();
            socketFailedValidation = destroyedByValidationCount - lastDestroyedByValidationCount;
            lastDestroyedByValidationCount = destroyedByValidationCount;

            socketInvalidated = connectionPool.getAndResetInvalidatedCount();
            activeSocket = connectionPool.getNumActive();
            idleSocket = connectionPool.getNumIdle();
        }

        long getSocketCreated() {
            return socketCreated;
        }

        long getSocketBorrowed() {
            return socketBorrowed;
        }

        long getSocketDestroyed() {
            return socketDestroyed;
        }

        long getSocketFailedValidation() {
            return socketFailedValidation;
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
