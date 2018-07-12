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


/**
 * @author jsgroth
 *         additional config parameters for LocalImhotepServiceCore with provided defaults
 */
public final class LocalImhotepServiceConfig {
    private int updateShardsFrequencySeconds = 120;
    private int heartBeatCheckFrequencySeconds = 60;
    private int syncShardStoreFrequencySeconds = 1200;
    private int cpuSlots = 0; // 0 = disabled
    private int cpuSchedulerHistoryLengthSeconds = 60;
    private int remoteFSIOSlots = 0;  // 0 = disabled
    private int remoteFSIOSchedulerHistoryLengthSeconds = 60;
    private int maxSessionsTotal = Integer.MAX_VALUE;
    private int maxSessionsPerUser = Integer.MAX_VALUE;
    private MetricStatsEmitter statsEmitter = MetricStatsEmitter.NULL_EMITTER;

    public int getUpdateShardsFrequencySeconds() {
        return updateShardsFrequencySeconds;
    }

    public int getHeartBeatCheckFrequencySeconds() {
        return heartBeatCheckFrequencySeconds;
    }

    public int getSyncShardStoreFrequencySeconds() {
        return syncShardStoreFrequencySeconds;
    }

    public LocalImhotepServiceConfig setUpdateShardsFrequencySeconds(final int updateShardsFrequencySeconds) {
        this.updateShardsFrequencySeconds = updateShardsFrequencySeconds;
        return this;
    }

    public LocalImhotepServiceConfig setHeartBeatCheckFrequencySeconds(final int heartBeatCheckFrequencySeconds) {
        this.heartBeatCheckFrequencySeconds = heartBeatCheckFrequencySeconds;
        return this;
    }

    public LocalImhotepServiceConfig setSyncShardStoreFrequencySeconds(final int syncShardStoreFrequencySeconds) {
        this.syncShardStoreFrequencySeconds = syncShardStoreFrequencySeconds;
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
}
