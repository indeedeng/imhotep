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
package com.indeed.imhotep.service;


import org.joda.time.Duration;

import javax.annotation.Nullable;

/**
 * @author jsgroth
 *         additional config parameters for LocalImhotepServiceCore with provided defaults
 */
public final class LocalImhotepServiceConfig {
    private long flamdexReaderCacheMaxDurationMillis = Duration.standardMinutes(5).getMillis();
    private int heartBeatCheckFrequencySeconds = 60;
    private int cpuSlots = 0; // 0 = disabled
    private int cpuSchedulerHistoryLengthSeconds = 60;
    private int remoteFSIOSlots = 0;  // 0 = disabled
    private int remoteFSIOSchedulerHistoryLengthSeconds = 60;
    private int maxSessionsTotal = Integer.MAX_VALUE;
    private int maxSessionsPerUser = Integer.MAX_VALUE;
    @Nullable
    private String advertisedHostName = null;
    private MetricStatsEmitter statsEmitter = MetricStatsEmitter.NULL_EMITTER;
    private boolean useArchiveBackend = false;
    @Nullable
    private ShardLocator dynamicShardLocator = null;


    public int getHeartBeatCheckFrequencySeconds() {
        return heartBeatCheckFrequencySeconds;
    }

    public LocalImhotepServiceConfig setHeartBeatCheckFrequencySeconds(final int heartBeatCheckFrequencySeconds) {
        this.heartBeatCheckFrequencySeconds = heartBeatCheckFrequencySeconds;
        return this;
    }

    public int getCpuSlots() {
        return cpuSlots;
    }

    public LocalImhotepServiceConfig setCpuSlots(int cpuSlots) {
        this.cpuSlots = cpuSlots;
        return this;
    }

    public int getRemoteFSIOSlots() {
        return remoteFSIOSlots;
    }

    public LocalImhotepServiceConfig setRemoteFSIOSlots(int remoteFSIOSlots) {
        this.remoteFSIOSlots = remoteFSIOSlots;
        return this;
    }

    public int getCpuSchedulerHistoryLengthSeconds() {
        return cpuSchedulerHistoryLengthSeconds;
    }

    public void setCpuSchedulerHistoryLengthSeconds(int cpuSchedulerHistoryLengthSeconds) {
        this.cpuSchedulerHistoryLengthSeconds = cpuSchedulerHistoryLengthSeconds;
    }

    public int getRemoteFSIOSchedulerHistoryLengthSeconds() {
        return remoteFSIOSchedulerHistoryLengthSeconds;
    }

    public void setRemoteFSIOSchedulerHistoryLengthSeconds(int remoteFSIOSchedulerHistoryLengthSeconds) {
        this.remoteFSIOSchedulerHistoryLengthSeconds = remoteFSIOSchedulerHistoryLengthSeconds;
    }

    public int getMaxSessionsTotal() {
        return maxSessionsTotal;
    }

    public LocalImhotepServiceConfig setMaxSessionsTotal(int maxSessionsTotal) {
        this.maxSessionsTotal = maxSessionsTotal;
        return this;
    }

    public int getMaxSessionsPerUser() {
        return maxSessionsPerUser;
    }

    public LocalImhotepServiceConfig setMaxSessionsPerUser(int maxSessionsPerUser) {
        this.maxSessionsPerUser = maxSessionsPerUser;
        return this;
    }

    public MetricStatsEmitter getStatsEmitter() {
        return statsEmitter;
    }

    public void setStatsEmitter(final MetricStatsEmitter statsEmitter) {
        this.statsEmitter = statsEmitter;
    }

    public long getFlamdexReaderCacheMaxDurationMillis() {
        return flamdexReaderCacheMaxDurationMillis;
    }

    public LocalImhotepServiceConfig setFlamdexReaderCacheMaxDurationMillis(long flamdexReaderCacheMaxDurationMillis) {
        this.flamdexReaderCacheMaxDurationMillis = flamdexReaderCacheMaxDurationMillis;
        return this;
    }

    @Nullable
    public String getAdvertisedHostName() {
        return advertisedHostName;
    }

    public LocalImhotepServiceConfig setAdvertisedHostName(final String advertisedHostName) {
        this.advertisedHostName = advertisedHostName;
        return this;
    }

    public boolean useArchiveBackend() {
        return useArchiveBackend;
    }

    public LocalImhotepServiceConfig setUseArchiveBackend(final boolean useArchiveBackend) {
        this.useArchiveBackend = useArchiveBackend;
        return this;
    }

    @Nullable
    public ShardLocator getDynamicShardLocator() {
        return dynamicShardLocator;
    }

    public LocalImhotepServiceConfig setDynamicShardLocator(@Nullable final ShardLocator dynamicShardLocator) {
        this.dynamicShardLocator = dynamicShardLocator;
        return this;
    }
}
