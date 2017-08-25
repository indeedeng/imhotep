package com.indeed.imhotep.api;

// todo: This is a first dummy version of perf stats. Think about what stats do we want from session.
public class PerformanceStats {
    public final long cpuTime;
    public final long maxMemoryUsage;

    public PerformanceStats(final long cpuTime, final long maxMemoryUsage) {
        this.cpuTime = cpuTime;
        this.maxMemoryUsage = maxMemoryUsage;
    }

    public static PerformanceStats combine(
            final PerformanceStats first,
            final PerformanceStats second) {
        if(first == null) {
            return second;
        }
        if(second == null) {
            return first;
        }

        return new PerformanceStats(
                first.cpuTime + second.cpuTime,
                first.maxMemoryUsage + second.maxMemoryUsage);
    }
}
