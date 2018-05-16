/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep.api;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public class PerformanceStats {
    public final long cpuTime;
    public final long maxMemoryUsage;
    public final long ftgsTempFileSize;
    public final long fieldFilesReadSize;
    public final long cpuSlotsExecTimeMs;
    public final long cpuSlotsWaitTimeMs;
    public final long ioSlotsExecTimeMs;
    public final long ioSlotsWaitTimeMs;
    public final ImmutableMap<String, Long> customStats;

    public PerformanceStats(
            final long cpuTime,
            final long maxMemoryUsage,
            final long ftgsTempFileSize,
            final long fieldFilesReadSize,
            long cpuSlotsExecTimeMs,
            long cpuSlotsWaitTimeMs,
            long ioSlotsExecTimeMs,
            long ioSlotsWaitTimeMs,
            final ImmutableMap<String, Long> customStats) {
        this.cpuTime = cpuTime;
        this.maxMemoryUsage = maxMemoryUsage;
        this.ftgsTempFileSize = ftgsTempFileSize;
        this.fieldFilesReadSize = fieldFilesReadSize;
        this.cpuSlotsExecTimeMs = cpuSlotsExecTimeMs;
        this.cpuSlotsWaitTimeMs = cpuSlotsWaitTimeMs;
        this.ioSlotsExecTimeMs = ioSlotsExecTimeMs;
        this.ioSlotsWaitTimeMs = ioSlotsWaitTimeMs;
        this.customStats = customStats;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long cpuTime = 0;
        private long maxMemoryUsage = 0;
        private long ftgsTempFileSize = 0;
        private long fieldFilesReadSize = 0;
        public long cpuSlotsExecTimeMs = 0;
        public long cpuSlotsWaitTimeMs = 0;
        public long ioSlotsExecTimeMs = 0;
        public long ioSlotsWaitTimeMs = 0;
        private final Map<String, Long> customStats = new HashMap<>();

        public void setCpuTime(final long cpuTime) {
            this.cpuTime = cpuTime;
        }

        public void setMaxMemoryUsage(final long maxMemoryUsage) {
            this.maxMemoryUsage = maxMemoryUsage;
        }

        public void setFtgsTempFileSize(final long ftgsTempFileSize) {
            this.ftgsTempFileSize = ftgsTempFileSize;
        }

        public void setFieldFilesReadSize(final long fieldFilesReadSize) {
            this.fieldFilesReadSize = fieldFilesReadSize;
        }

        public long getCpuTime() {
            return cpuTime;
        }

        public long getMaxMemoryUsage() {
            return maxMemoryUsage;
        }

        public long getFtgsTempFileSize() {
            return ftgsTempFileSize;
        }

        public long getFieldFilesReadSize() {
            return fieldFilesReadSize;
        }

        public long getCpuSlotsExecTimeMs() {
            return cpuSlotsExecTimeMs;
        }

        public void setCpuSlotsExecTimeMs(long cpuSlotsExecTimeMs) {
            this.cpuSlotsExecTimeMs = cpuSlotsExecTimeMs;
        }

        public long getCpuSlotsWaitTimeMs() {
            return cpuSlotsWaitTimeMs;
        }

        public void setCpuSlotsWaitTimeMs(long cpuSlotsWaitTimeMs) {
            this.cpuSlotsWaitTimeMs = cpuSlotsWaitTimeMs;
        }

        public long getIoSlotsExecTimeMs() {
            return ioSlotsExecTimeMs;
        }

        public void setIoSlotsExecTimeMs(long ioSlotsExecTimeMs) {
            this.ioSlotsExecTimeMs = ioSlotsExecTimeMs;
        }

        public long getIoSlotsWaitTimeMs() {
            return ioSlotsWaitTimeMs;
        }

        public void setIoSlotsWaitTimeMs(long ioSlotsWaitTimeMs) {
            this.ioSlotsWaitTimeMs = ioSlotsWaitTimeMs;
        }

        public Map<String, Long> getCustomStats() {
            return customStats;
        }

        public void add(final PerformanceStats stats) {
            if(stats == null) {
                return;
            }

            cpuTime += stats.cpuTime;
            maxMemoryUsage += stats.maxMemoryUsage;
            fieldFilesReadSize += stats.fieldFilesReadSize;
            ftgsTempFileSize += stats.ftgsTempFileSize;
            cpuSlotsExecTimeMs += stats.cpuSlotsExecTimeMs;
            cpuSlotsWaitTimeMs += stats.cpuSlotsWaitTimeMs;
            ioSlotsExecTimeMs += stats.ioSlotsExecTimeMs;
            ioSlotsWaitTimeMs += stats.ioSlotsWaitTimeMs;


            for(final Map.Entry<String, Long> entry : stats.customStats.entrySet()) {
                Long value = customStats.get(entry.getKey());
                value = (value != null) ? (value + entry.getValue()) : entry.getValue();
                customStats.put(entry.getKey(), value);
            }
        }

        public PerformanceStats build() {
            return new PerformanceStats(
                    cpuTime,
                    maxMemoryUsage,
                    ftgsTempFileSize,
                    fieldFilesReadSize,
                    cpuSlotsExecTimeMs, cpuSlotsWaitTimeMs, ioSlotsExecTimeMs, ioSlotsWaitTimeMs, ImmutableMap.copyOf(customStats));
        }
    }
}
