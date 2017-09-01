package com.indeed.imhotep.api;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public class PerformanceStats {
    public final long cpuTime;
    public final long maxMemoryUsage;
    public final long ftgsTempFileSize;
    public final long fieldFilesFeadSize;
    public final ImmutableMap<String, String> customStats;

    public PerformanceStats(
            final long cpuTime,
            final long maxMemoryUsage,
            final long ftgsTempFileSize,
            final long fieldFilesFeadSize,
            final ImmutableMap<String, String> customStats) {
        this.cpuTime = cpuTime;
        this.maxMemoryUsage = maxMemoryUsage;
        this.ftgsTempFileSize = ftgsTempFileSize;
        this.fieldFilesFeadSize = fieldFilesFeadSize;
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

        final Map<String, String> joinedStats = new HashMap<>();
        joinedStats.putAll(first.customStats);
        for (final Map.Entry<String, String> entry : second.customStats.entrySet() ) {
            String value = joinedStats.get(entry.getKey());
            value = (value != null) ? (value + "\n" + entry.getValue()) : entry.getValue();
            joinedStats.put(entry.getKey(), value);
        }

        return new PerformanceStats(
                first.cpuTime + second.cpuTime,
                first.maxMemoryUsage + second.maxMemoryUsage,
                first.ftgsTempFileSize + second.ftgsTempFileSize,
                first.fieldFilesFeadSize + second.fieldFilesFeadSize,
                ImmutableMap.copyOf(joinedStats));
    }
}
