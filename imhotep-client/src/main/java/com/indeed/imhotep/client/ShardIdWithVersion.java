/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.client;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.ShardInfo;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * @author jsgroth
 */
public final class ShardIdWithVersion implements Comparable<ShardIdWithVersion> {
    public static final Function<ShardIdWithVersion, String> SHARD_ID_GETTER = new Function<ShardIdWithVersion, String>() {
        @Override
        public String apply(@Nullable final ShardIdWithVersion input) {
            assert input != null;
            return input.getShardId();
        }
    };

    public static final Function<ShardIdWithVersion, Long> VERSION_GETTER = new Function<ShardIdWithVersion, Long>() {
        @Override
        public Long apply(@Nullable final ShardIdWithVersion input) {
            assert input != null;
            return input.getVersion();
        }
    };

    private final String shardId;
    private final long version;

    private ShardInfo.DateTimeRange range;    // lazily computed

    public ShardIdWithVersion(final String shardId, final long version) {
        this.shardId = shardId;
        this.version = version;
    }

    public String getShardId() {
        return shardId;
    }

    public long getVersion() {
        return version;
    }

    public DateTime getStart() {
        if(range == null) { // this is not thread safe but the operation should be deterministic with no side effects
            range = ShardInfo.parseDateTime(shardId);
        }
        return range.start;
    }

    public DateTime getEnd() {
        if(range == null) {
            range = ShardInfo.parseDateTime(shardId);
        }
        return range.end;
    }

    public ShardInfo.DateTimeRange getRange() {
        if(range == null) {
            range = ShardInfo.parseDateTime(shardId);
        }
        return range;
    }

    @Override
    public int compareTo(@Nonnull final ShardIdWithVersion o) {
        final int c = shardId.compareTo(o.shardId);
        if (c != 0) {
            return c;
        }
        return Longs.compare(version, o.version);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ShardIdWithVersion that = (ShardIdWithVersion) o;

        if (version != that.version) {
            return false;
        }
        return shardId != null ? shardId.equals(that.shardId) : that.shardId == null;
    }

    @Override
    public int hashCode() {
        int result = shardId != null ? shardId.hashCode() : 0;
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ShardIdWithVersion{" +
                "shardId='" + shardId + '\'' +
                ", version=" + version +
                '}';
    }

    public static List<String> keepShardIds(final List<ShardIdWithVersion> shards) {
        final List<String> result = Lists.newArrayListWithCapacity(shards.size());
        for(final ShardIdWithVersion shard : shards) {
            result.add(shard.getShardId());
        }
        return result;
    }
}
