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
    public final List<ShardDump> shards;

    public ImhotepStatusDump(long usedMemory, long totalMemory, List<SessionDump> openSessions, List<ShardDump> shards) {
        this.usedMemory = usedMemory;
        this.totalMemory = totalMemory;
        this.openSessions = openSessions;
        this.shards = shards;
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

    public List<ShardDump> getShards() {
        return shards;
    }

    public StatusDumpMessage toProto() {
        final StatusDumpMessage.Builder builder = StatusDumpMessage.newBuilder()
                .setUsedMemory(usedMemory)
                .setTotalMemory(totalMemory);

        for (final SessionDump sessionDump : openSessions) {
            builder.addOpenSession(sessionDump.toProto());
        }

        for (final ShardDump shardDump : shards) {
            builder.addLoadedShard(shardDump.toProto());
        }

        return builder.build();
    }

    public static ImhotepStatusDump fromProto(StatusDumpMessage protoDump) {
        final long usedMemory = protoDump.getUsedMemory();
        final long totalMemory = protoDump.getTotalMemory();
        final List<SessionDump> openSessions = new ArrayList<SessionDump>(protoDump.getOpenSessionCount());
        for (final SessionDumpMessage protoSessionDump : protoDump.getOpenSessionList()) {
            openSessions.add(SessionDump.fromProto(protoSessionDump));
        }
        final List<ShardDump> shards = new ArrayList<ShardDump>(protoDump.getLoadedShardCount());
        for (final ShardDumpMessage shardDump : protoDump.getLoadedShardList()) {
            shards.add(ShardDump.fromProto(shardDump));
        }
        return new ImhotepStatusDump(usedMemory, totalMemory, openSessions, shards);
    }

    public static class SessionDump {
        public final String sessionId;
        public final String dataset;
        public final String hostname;
        public final String username;
        public final String ipAddress;
        public final int clientVersion;
        public final List<ShardDump> openShards;

        public SessionDump(String sessionId, String dataset, String hostname, String username, String ipAddress, int clientVersion, List<ShardDump> openShards) {
            this.sessionId = sessionId;
            this.dataset = dataset;
            this.hostname = hostname;
            this.username = username;
            this.ipAddress = ipAddress;
            this.clientVersion = clientVersion;
            this.openShards = openShards;
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

        public List<ShardDump> getOpenShards() {
            return openShards;
        }

        public SessionDumpMessage toProto() {
            final SessionDumpMessage.Builder builder = SessionDumpMessage.newBuilder()
                    .setSessionId(sessionId)
                    .setDataset(dataset)
                    .setHostname(hostname)
                    .setUsername(username)
                    .setIpAddress(ipAddress)
                    .setClientVersion(clientVersion);

            for (final ShardDump shardDump : openShards) {
                builder.addOpenShard(shardDump.toProto());
            }

            return builder.build();
        }

        public static SessionDump fromProto(SessionDumpMessage protoDump) {
            final List<ShardDump> openShards = new ArrayList<ShardDump>(protoDump.getOpenShardCount());
            for (final ShardDumpMessage shardDump : protoDump.getOpenShardList()) {
                openShards.add(ShardDump.fromProto(shardDump));
            }
            return new SessionDump(protoDump.getSessionId(), protoDump.getDataset(), protoDump.getHostname(),
                    protoDump.getUsername(), protoDump.getIpAddress(), protoDump.getClientVersion(), openShards);
        }
    }

    public static class MetricDump {
        public final String metric;
        public final long memoryUsed;

        public MetricDump(String metric, long memoryUsed) {
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

        public static MetricDump fromProto(MetricDumpMessage protoDump) {
            return new MetricDump(protoDump.getMetric(), protoDump.getMemoryUsed());
        }
    }

    public static class ShardDump {
        public final String shardId;
        public final String dataset;
        public final int numDocs;
        public final List<MetricDump> loadedMetrics;

        public ShardDump(String shardId, String dataset, int numDocs, List<MetricDump> loadedMetrics) {
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

        public static ShardDump fromProto(ShardDumpMessage protoDump) {
            final List<MetricDump> loadedMetrics = new ArrayList<MetricDump>(protoDump.getLoadedMetricCount());
            for (final MetricDumpMessage protoMetricDump : protoDump.getLoadedMetricList()) {
                loadedMetrics.add(MetricDump.fromProto(protoMetricDump));
            }
            return new ShardDump(protoDump.getShardId(), protoDump.getDataset(), protoDump.getNumDocs(), loadedMetrics);
        }
    }
}
