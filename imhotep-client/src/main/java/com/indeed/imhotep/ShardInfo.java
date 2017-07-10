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
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collection;

/**
 * @author jsgroth
 */
public class ShardInfo implements Comparable<ShardInfo> {
    private static final Logger log = Logger.getLogger(ShardInfo.class);
    private static final DateTimeFormatter yyyymmdd = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHours(-6));
    private static final DateTimeFormatter yyyymmddhh = DateTimeFormat.forPattern("yyyyMMdd.HH").withZone(DateTimeZone.forOffsetHours(-6));

    public final String dataset;
    public final String shardId;
    public final Collection<String> loadedMetrics;
    public final int numDocs;
    public final long version;

    private DateTimeRange range;    // lazily computed

    public ShardInfo(String dataset, String shardId, Collection<String> loadedMetrics, int numDocs, long version) {
        this.dataset = dataset;
        this.shardId = shardId;
        this.loadedMetrics = loadedMetrics;
        this.numDocs = numDocs;
        this.version = version;
    }

    public String getDataset() {
        return dataset;
    }

    public String getShardId() {
        return shardId;
    }

    public Collection<String> getLoadedMetrics() {
        return loadedMetrics;
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

    public static ShardInfo fromProto(ShardInfoMessage protoShard) {
        return new ShardInfo(
                protoShard.getDataset(),
                protoShard.getShardId(),
                protoShard.getLoadedMetricList(),
                protoShard.getNumDocs(),
                protoShard.getVersion()
        );
    }

    public ShardInfoMessage toProto() {
        return ShardInfoMessage.newBuilder()
                .setDataset(dataset)
                .setShardId(shardId)
                .addAllLoadedMetric(loadedMetrics)
                .setNumDocs(numDocs)
                .setVersion(version)
                .build();
    }

    @Override
    public int compareTo(ShardInfo o) {
        final int c = dataset.compareTo(o.dataset);
        if (c != 0) return c;
        final int c2 = shardId.compareTo(o.shardId);
        if (c2 != 0) return c2;
        return Longs.compare(version, o.version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardInfo shardInfo = (ShardInfo) o;

        if (numDocs != shardInfo.numDocs) return false;
        if (version != shardInfo.version) return false;
        if (dataset != null ? !dataset.equals(shardInfo.dataset) : shardInfo.dataset != null) return false;
        if (loadedMetrics != null ? !loadedMetrics.equals(shardInfo.loadedMetrics) : shardInfo.loadedMetrics != null)
            return false;
        if (shardId != null ? !shardId.equals(shardInfo.shardId) : shardInfo.shardId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dataset != null ? dataset.hashCode() : 0;
        result = 31 * result + (shardId != null ? shardId.hashCode() : 0);
        result = 31 * result + (loadedMetrics != null ? loadedMetrics.hashCode() : 0);
        result = 31 * result + numDocs;
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "["+dataset+":"+shardId+"]";
    }

    public static DateTimeRange parseDateTime(String shardId) {
        final Interval interval = ShardTimeUtils.parseInterval(shardId);
        return new DateTimeRange(interval.getStart(), interval.getEnd());
    }

    public static final class DateTimeRange {
        public final DateTime start;
        public final DateTime end;

        public DateTimeRange(DateTime start, DateTime end) {
            this.start = start;
            this.end = end;
        }
    }
}
