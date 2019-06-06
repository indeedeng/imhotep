package com.indeed.imhotep.connection;

import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.scheduling.SilentCloseable;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.threads.NamedThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ImhotepMemoryStatsReporter implements SilentCloseable {
    private final ScheduledExecutorService reportExecutor;

    private final MetricStatsEmitter statsEmitter;
    private final MemoryReserver memoryReserver;

    public ImhotepMemoryStatsReporter(
            final MetricStatsEmitter statsEmitter,
            final MemoryReserver memoryReserver
    ) {
        this.statsEmitter = statsEmitter;
        this.memoryReserver = memoryReserver;
        this.reportExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("imhotepMemoryPoolReporter"));
        reportExecutor.scheduleAtFixedRate(this::report, 1, 1, TimeUnit.SECONDS);
    }

    private void report() {
        statsEmitter.gauge("memory.reserver.used", memoryReserver.usedMemory());
        statsEmitter.gauge("memory.reserver.total", memoryReserver.totalMemory());
    }

    @Override
    public void close() {
        reportExecutor.shutdown();
    }
}
