package com.indeed.imhotep.scheduling;

import com.indeed.imhotep.service.MetricStatsEmitter;

import java.io.Closeable;

/**
 * Used only for testing
 */
class NoopTaskScheduler extends TaskScheduler {
    NoopTaskScheduler() {
        super(0, 0, 0, null, MetricStatsEmitter.NULL_EMITTER);
    }

    @Override
    public synchronized SilentCloseable lockSlot() {
        return () -> {};
    }

    @Override
    public synchronized Closeable temporaryUnlock() {
        return () -> {};
    }

    @Override
    protected void initializeSchedulers(SchedulerType schedulerType) {
        // noop
    }
}
