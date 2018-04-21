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

package com.indeed.imhotep.shardmaster;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author vladimir
 */
@ThreadSafe
class TimeBasedShardAssigner implements ShardAssigner {
    private static final Logger LOGGER = Logger.getLogger(InMemoryShardAssignmentInfoDao.class);
    private final MinHashShardAssigner minHashShardAssigner;

    private static final ThreadLocal<HashFunction> HASH_FUNCTION = new ThreadLocal<HashFunction>() {
        @Override
        protected HashFunction initialValue() {
            return Hashing.murmur3_32((int)1515546902721L);
        }
    };

    TimeBasedShardAssigner() {
        // TODO: replication support?
        minHashShardAssigner = new MinHashShardAssigner(1);
    }

    @SuppressWarnings("Guava")
    @Override
    public Iterable<ShardAssignmentInfo> assign(final List<Host> hosts, final String dataset, final Iterable<ShardDir> shards) {
        // TODO: handle downtimed servers. currently expected to be passed in as Null
        final List<Host> upHosts = hosts.stream().filter(Objects::nonNull).collect(Collectors.toList());
        int initialServerNumberForDataset = (int)Math.abs((long)HASH_FUNCTION.get().hashString(dataset, Charsets.UTF_8).asInt()) % hosts.size();

        return FluentIterable.from(shards).transformAndConcat(new Function<ShardDir, Iterable<ShardAssignmentInfo>>() {
            @Override
            public Iterable<ShardAssignmentInfo> apply(final ShardDir shard) {
                final String shardId = shard.getId();
                final long shardIndex = Math.abs(getShardIndexForShardSize(shardId));
                final int assignedServerNumber = (int)((initialServerNumberForDataset + shardIndex) % hosts.size());
                final Host assignedServer = hosts.get(assignedServerNumber);
                if(assignedServer == null) {
                    // this server is in downtime so assign the shard to another server using minhashing
                    return minHashShardAssigner.assign(upHosts, dataset, Collections.singletonList(shard));
                }


                return Collections.singletonList(new ShardAssignmentInfo(dataset,
                        shard.getIndexDir().toUri().toString(),
                        assignedServer));
            }
        });
    }

    private long getShardIndexForShardSize(String shardId) {
        final Interval shardTimeRange = ShardTimeUtils.parseInterval(shardId);
        return getShardIndexForShardSize(shardTimeRange);
    }

    /**
     * We assign each shard an index while leaving space for missing shards with the same duration.
     * Shards aligned to months and years are special-cased because their duration in milliseconds is not consistent.
     */
    private long getShardIndexForShardSize(Interval shardTimeRange) {
        final DateTime start = shardTimeRange.getStart();
        final DateTime end = shardTimeRange.getEnd();
        if(start.equals(end)) {
            LOGGER.warn("Invalid shard with end = start = " + start);
        }
        if(dateIsMonthAligned(start) && dateIsMonthAligned(end)) {
            if(dateIsYearAligned(start) && dateIsYearAligned(end)) {
                // shard is year aligned
                long startYearIndex = start.getYear();
                long endYearIndex = end.getYear();
                long shardDuration = endYearIndex - startYearIndex;
                return startYearIndex / shardDuration;
            }

            // shard is month aligned
            long startMonthIndex = start.getYear() * 12 + start.getMonthOfYear();
            long endMonthIndex = end.getYear() * 12 + end.getMonthOfYear();
            long shardDuration = endMonthIndex - startMonthIndex;
            return startMonthIndex / shardDuration;
        }

        // shard should have a regular duration so use that as the unit
        long shardDurationMillis = shardTimeRange.toDurationMillis();
        if(shardDurationMillis < 0) {
            LOGGER.warn("Illegal shard with end time before the start time. Start: " + start + ", end: " + end);
        }
        if (shardDurationMillis <= 0) {
            // we've logged a warning but don't want it to fail assignment
            shardDurationMillis = TimeUnit.HOURS.toMillis(1);
        }
        return shardTimeRange.getStartMillis() / shardDurationMillis;
    }

    /**
     * Returns true if dateTime is at midnight on the first of the month in UTC-6
     */
    private boolean dateIsMonthAligned(DateTime dateTime) {
        return dateTime.getDayOfMonth() == 1 && dateTime.getMillisOfDay() == 0;
    }

    /**
     * Returns true if dateTime is at new year in UTC-6.
     * dateTime must be month aligned.
     */
    private boolean dateIsYearAligned(DateTime dateTime) {
        return dateTime.getMonthOfYear() == 1;
    }
}
