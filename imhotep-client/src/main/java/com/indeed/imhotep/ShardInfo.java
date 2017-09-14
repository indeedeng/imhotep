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
 package com.indeed.imhotep;

import com.google.common.primitives.Longs;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.protobuf.ShardInfoMessage;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;

/**
 * @author jsgroth
 */
public class ShardInfo implements Comparable<ShardInfo> {
    public final String shardId;
    public final int numDocs;
    public final long version;

    private DateTimeRange range;    // lazily computed

    public ShardInfo(final String shardId, final int numDocs, final long version) {
        this.shardId = shardId;
        this.numDocs = numDocs;
        this.version = version;
    }

    public String getShardId() {
        return shardId;
    }

    public int getNumDocs() {
        return numDocs;
    }

    public long getVersion() {
        return version;
    }

    public DateTime getStart() {
        if(range == null) { // this is not thread safe but the operation should be deterministic with no side effects
            range = parseDateTime(shardId);
        }
        return range.start;
    }

    public DateTime getEnd() {
        if(range == null) {
            range = parseDateTime(shardId);
        }
        return range.end;
    }

    public DateTimeRange getRange() {
        if(range == null) {
            range = parseDateTime(shardId);
        }
        return range;
    }

    public static ShardInfo fromProto(final ShardInfoMessage protoShard) {
        return new ShardInfo(
                protoShard.getShardId(),
                protoShard.getNumDocs(),
                protoShard.getVersion()
        );
    }

    public ShardInfoMessage toProto() {
        return ShardInfoMessage.newBuilder()
                .setShardId(shardId)
                .setNumDocs(numDocs)
                .setVersion(version)
                .build();
    }

    @Override
    public int compareTo(@Nonnull final ShardInfo o) {
        final int c2 = shardId.compareTo(o.shardId);
        if (c2 != 0) {
            return c2;
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

        final ShardInfo shardInfo = (ShardInfo) o;

        if (numDocs != shardInfo.numDocs) {
            return false;
        }
        if (version != shardInfo.version) {
            return false;
        }
        if (shardId != null ? !shardId.equals(shardInfo.shardId) : shardInfo.shardId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = shardId != null ? shardId.hashCode() : 0;
        result = 31 * result + numDocs;
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return shardId;
    }

    public static DateTimeRange parseDateTime(final String shardId) {
        try {
            final Interval interval = ShardTimeUtils.parseInterval(shardId);
            return new DateTimeRange(interval.getStart(), interval.getEnd());
        } catch (final IllegalArgumentException e) {
            return null;
        }
    }

    public static final class DateTimeRange {
        public final DateTime start;
        public final DateTime end;

        public DateTimeRange(final DateTime start, final DateTime end) {
            this.start = start;
            this.end = end;
        }
    }
}
