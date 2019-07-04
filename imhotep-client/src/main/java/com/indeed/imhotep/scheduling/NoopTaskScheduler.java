package com.indeed.imhotep.scheduling;

import com.indeed.imhotep.service.MetricStatsEmitter;

import javax.annotation.Nonnull;

/**
 * Used only for testing
 */
class NoopTaskScheduler extends TaskScheduler {
    NoopTaskScheduler() {
        super(0, 0, 0, 0, null, MetricStatsEmitter.NULL_EMITTER);
    }

    @Nonnull
    @Override
    public synchronized SilentCloseable lockSlot() {
        return () -> {};
    }

    @Nonnull
    @Override
    public synchronized SilentCloseable temporaryUnlock() {
        return () -> {};
    }

    @Override
    protected void initializeSchedulers(SchedulerType schedulerType) {
        // noop
    }
}
