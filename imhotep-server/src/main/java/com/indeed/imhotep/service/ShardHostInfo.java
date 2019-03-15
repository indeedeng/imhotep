package com.indeed.imhotep.service;

import com.google.common.base.Objects;
import com.indeed.imhotep.client.Host;

import javax.annotation.Nullable;

/**
 * @author xweng
 */
public class ShardHostInfo {

    private final String shardName;

    private final Host shardServer;

    @Nullable private final Host shardOwner;

    public ShardHostInfo(final String shardName, final Host shardServer, @Nullable final Host shardOwner) {
        this.shardName = shardName;
        this.shardServer = shardServer;
        this.shardOwner = shardOwner;
    }

    public String getShardName() {
        return shardName;
    }

    public Host getShardServer() {
        return shardServer;
    }

    public Host getShardOwner() {
        return shardOwner;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ShardHostInfo that = (ShardHostInfo) o;
        return Objects.equal(shardName, that.shardName) &&
                Objects.equal(shardServer, that.shardServer) &&
                Objects.equal(shardOwner, that.shardOwner);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(shardName, shardServer, shardOwner);
    }

    @Override
    public String toString() {
        return "ShardHostInfo{" +
                "shardName='" + shardName + '\'' +
                ", shardServer=" + shardServer +
                ", shardOwner=" + shardOwner +
                '}';
    }
}
