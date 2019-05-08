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
 package com.indeed.imhotep;

import com.indeed.imhotep.protobuf.MetricDumpMessage;
import com.indeed.imhotep.protobuf.SessionDumpMessage;
import com.indeed.imhotep.protobuf.ShardDumpMessage;
import com.indeed.imhotep.protobuf.StatusDumpMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jsgroth
 */
public class ImhotepStatusDump {
    public final long usedMemory;
    public final long totalMemory;
    public final List<SessionDump> openSessions;

    public ImhotepStatusDump(final long usedMemory, final long totalMemory, final List<SessionDump> openSessions) {
        this.usedMemory = usedMemory;
        this.totalMemory = totalMemory;
        this.openSessions = openSessions;
    }

    public long getUsedMemory() {
        return usedMemory;
    }

    public long getTotalMemory() {
        return totalMemory;
    }

    public List<SessionDump> getOpenSessions() {
        return openSessions;
    }

    public StatusDumpMessage toProto() {
        final StatusDumpMessage.Builder builder = StatusDumpMessage.newBuilder()
                .setUsedMemory(usedMemory)
                .setTotalMemory(totalMemory);

        for (final SessionDump sessionDump : openSessions) {
            builder.addOpenSession(sessionDump.toProto());
        }

        return builder.build();
    }

    public static ImhotepStatusDump fromProto(final StatusDumpMessage protoDump) {
        final long usedMemory = protoDump.getUsedMemory();
        final long totalMemory = protoDump.getTotalMemory();
        final List<SessionDump> openSessions = new ArrayList<>(protoDump.getOpenSessionCount());
        for (final SessionDumpMessage protoSessionDump : protoDump.getOpenSessionList()) {
            openSessions.add(SessionDump.fromProto(protoSessionDump));
        }
        return new ImhotepStatusDump(usedMemory, totalMemory, openSessions);
    }

    public static class SessionDump {
        public final String sessionId;
        public final String dataset;
        public final String hostname;
        public final String username;
        public final String clientName;
        public final String ipAddress;
        public final int clientVersion;
        public final long creationTime;
        public final List<ShardDump> openShards;
        public final long usedMemory;
        public final long maxUsedMemory;
        public final byte priority;

        public SessionDump(
                final String sessionId,
                final String dataset,
                final String hostname,
                final String username,
                final String clientName,
                final String ipAddress,
                final int clientVersion,
                final long creationTime,
                final List<ShardDump> openShards,
                final long usedMemory,
                final long maxUsedMemory,
                final byte priority) {
            this.sessionId = sessionId;
            this.dataset = dataset;
            this.hostname = hostname;
            this.username = username;
            this.clientName = clientName;
            this.ipAddress = ipAddress;
            this.clientVersion = clientVersion;
            this.creationTime = creationTime;
            this.openShards = openShards;
            this.usedMemory = usedMemory;
            this.maxUsedMemory = maxUsedMemory;
            this.priority = priority;
        }

        public String getSessionId() {
            return sessionId;
        }

        public String getDataset() {
            return dataset;
        }

        @Deprecated
        public String getHostname() {
            return hostname;
        }

        public String getUsername() {
            return username;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public int getClientVersion() {
            return clientVersion;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public String getClientName() {
            return clientName;
        }

        public long getUsedMemory() {
            return usedMemory;
        }

        public long getMaxUsedMemory() {
            return maxUsedMemory;
        }

        public List<ShardDump> getOpenShards() {
            return openShards;
        }

        public byte getPriority() {
            return priority;
        }

        public SessionDumpMessage toProto() {
            final SessionDumpMessage.Builder builder = SessionDumpMessage.newBuilder()
                    .setSessionId(sessionId)
                    .setDataset(dataset)
                    .setHostname(hostname)
                    .setUsername(username)
                    .setIpAddress(ipAddress)
                    .setClientVersion(clientVersion)
                    .setCreationTime(creationTime)
                    .setClientName(clientName)
                    .setUsedMemory(usedMemory)
                    .setMaxUsedMemory(maxUsedMemory)
                    .setPriority(priority);

            for (final ShardDump shardDump : openShards) {
                builder.addOpenShard(shardDump.toProto());
            }

            return builder.build();
        }

        public static SessionDump fromProto(final SessionDumpMessage protoDump) {
            final List<ShardDump> openShards = new ArrayList<>(protoDump.getOpenShardCount());
            for (final ShardDumpMessage shardDump : protoDump.getOpenShardList()) {
                openShards.add(ShardDump.fromProto(shardDump));
            }
            return new SessionDump(protoDump.getSessionId(), protoDump.getDataset(), protoDump.getHostname(),
                    protoDump.getUsername(), protoDump.getClientName(), protoDump.getIpAddress(), protoDump.getClientVersion(),
                    protoDump.getCreationTime(), openShards, protoDump.getUsedMemory(), protoDump.getMaxUsedMemory(),
                    (byte)protoDump.getPriority());
        }
    }

    public static class MetricDump {
        public final String metric;
        public final long memoryUsed;

        public MetricDump(final String metric, final long memoryUsed) {
            this.metric = metric;
            this.memoryUsed = memoryUsed;
        }

        public String getMetric() {
            return metric;
        }

        public long getMemoryUsed() {
            return memoryUsed;
        }

        public MetricDumpMessage toProto() {
            return MetricDumpMessage.newBuilder()
                    .setMetric(metric)
                    .setMemoryUsed(memoryUsed)
                    .setRefCount(1)
                    .build();
        }

        public static MetricDump fromProto(final MetricDumpMessage protoDump) {
            return new MetricDump(protoDump.getMetric(), protoDump.getMemoryUsed());
        }
    }

    public static class ShardDump {
        public final String shardId;
        public final String dataset;
        public final int numDocs;
        public final List<MetricDump> loadedMetrics;

        public ShardDump(final String shardId, final String dataset, final int numDocs, final List<MetricDump> loadedMetrics) {
            this.shardId = shardId;
            this.dataset = dataset;
            this.numDocs = numDocs;
            this.loadedMetrics = loadedMetrics;
        }

        public String getShardId() {
            return shardId;
        }

        public String getDataset() {
            return dataset;
        }

        public int getNumDocs() {
            return numDocs;
        }

        public List<MetricDump> getLoadedMetrics() {
            return loadedMetrics;
        }

        public ShardDumpMessage toProto() {
            final ShardDumpMessage.Builder builder = ShardDumpMessage.newBuilder()
                    .setShardId(shardId)
                    .setDataset(dataset)                    
                    .setNumDocs(numDocs);

            for (final MetricDump metricDump : loadedMetrics) {
                builder.addLoadedMetric(metricDump.toProto());
            }

            return builder.build();
        }

        public static ShardDump fromProto(final ShardDumpMessage protoDump) {
            final List<MetricDump> loadedMetrics = new ArrayList<>(protoDump.getLoadedMetricCount());
            for (final MetricDumpMessage protoMetricDump : protoDump.getLoadedMetricList()) {
                loadedMetrics.add(MetricDump.fromProto(protoMetricDump));
            }
            return new ShardDump(protoDump.getShardId(), protoDump.getDataset(), protoDump.getNumDocs(), loadedMetrics);
        }
    }
}
