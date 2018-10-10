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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ShardMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class Shard extends ShardInfo {
    public final Host server;

    public Shard(final String shardId, final int numDocs, final long version, final Host host) {
        super(shardId, numDocs, version);
        this.server = host;
    }

    public Shard(final String shardId, final int numDocs, final long version, final Host host, @Nullable final String fileName) {
        super(fileName, shardId, numDocs, version);
        this.server = host;
    }

    public Shard(final ShardInfo shardInfo, final Host host) {
        super(shardInfo.fileName, shardInfo.shardId, shardInfo.numDocs, shardInfo.version);
        this.server = host;
    }

    /** Only used in ShardLoaderUtil.findShards() in pigutil, Imhotep proper should always provide a host. */
    public Shard(final String shardId, final int numDocs, final long version) {
        super(shardId, numDocs, version);
        this.server = null;
    }

    public String getFileName() {
        if (fileName == null) {
            return this.shardId + "." + this.version;
        } else {
            return fileName;
        }
    }

    public Host getServer() {
        return server;
    }

    /** Note that servers are not used in comparisons */
    @Override
    public int compareTo(@Nonnull final ShardInfo o) {
        return super.compareTo(o);
    }

    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    public static List<String> keepShardIds(final List<Shard> shards) {
        final List<String> result = Lists.newArrayListWithCapacity(shards.size());
        for(final Shard shard : shards) {
            result.add(shard.getShardId());
        }
        return result;
    }

    public Shard withHost(final Host newHost) {
        return new Shard(shardId, numDocs, version, newHost, fileName);
    }

    public static Shard fromShardMessage(final ShardMessage message) {
        final Host host = new Host(message.getHost().getHost(), message.getHost().getPort());
        if (message.hasPath()) {
            return new Shard(message.getShardId(), message.getNumDocs(), message.getVersion(), host, message.getPath());
        } else {
            return new Shard(message.getShardId(), message.getNumDocs(), message.getVersion(), host);
        }
    }

    public ShardMessage.Builder addToShardMessage(final ShardMessage.Builder builder) {
        Preconditions.checkNotNull(server);
        builder
                .setHost(HostAndPort.newBuilder().setHost(server.hostname).setPort(server.port))
                .setShardId(shardId)
                .setNumDocs(numDocs)
                .setVersion(version);
        if (fileName != null) {
            builder.setPath(fileName);
        }
        return builder;
    }
}
