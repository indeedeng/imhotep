package com.indeed.imhotep.api;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public class PerformanceStats {
    public final long cpuTime;
    public final long maxMemoryUsage;
    public final long ftgsTempFileSize;
    public final long fieldFilesReadSize;
    public final ImmutableMap<String, Long> customStats;

    public PerformanceStats(
            final long cpuTime,
            final long maxMemoryUsage,
            final long ftgsTempFileSize,
            final long fieldFilesFeadSize,
            final ImmutableMap<String, Long> customStats) {
        this.cpuTime = cpuTime;
        this.maxMemoryUsage = maxMemoryUsage;
        this.ftgsTempFileSize = ftgsTempFileSize;
        this.fieldFilesReadSize = fieldFilesFeadSize;
        this.customStats = customStats;
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

        final Map<String, Long> joinedStats = new HashMap<>();
        joinedStats.putAll(first.customStats);
        for (final Map.Entry<String, Long> entry : second.customStats.entrySet() ) {
            Long value = joinedStats.get(entry.getKey());
            value = (value != null) ? (value + entry.getValue()) : entry.getValue();
            joinedStats.put(entry.getKey(), value);
        }

        return new PerformanceStats(
                first.cpuTime + second.cpuTime,
                first.maxMemoryUsage + second.maxMemoryUsage,
                first.ftgsTempFileSize + second.ftgsTempFileSize,
                first.fieldFilesReadSize + second.fieldFilesReadSize,
                ImmutableMap.copyOf(joinedStats));
    }
}
