package com.indeed.imhotep.scheduling;

import com.indeed.imhotep.service.MetricStatsEmitter;

import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * Used only for testing
 */
class NoopTaskScheduler extends TaskScheduler {
    NoopTaskScheduler() {
        super(0, 0, 0, null, MetricStatsEmitter.NULL_EMITTER);
    }

    @Nullable
    @Override
    public synchronized SilentCloseable lockSlot() {
        return null;
    }

    @Nullable
    @Override
    public synchronized Closeable temporaryUnlock() {
        return null;
    }

    @Override
    protected void initializeSchedulers(SchedulerType schedulerType) {
        // noop
    }
}
