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

import com.indeed.imhotep.ShardInfo;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
* @author jsgroth
*/
public class DateRangeShardFilter implements ShardFilter {

    private final DateTime minDate;
    private final DateTime maxDate;

    public DateRangeShardFilter(DateTime startDate, DateTime endDate) {
        this.minDate = startDate;
        this.maxDate = endDate;
    }

    @Override
    public boolean accept(ShardInfo shard) {
        final String shardId = shard.getShardId();
        final Interval interval = ShardTimeUtils.parseInterval(shardId);
        return interval.getEnd().isAfter(minDate) && interval.getStart().isBefore(maxDate);
    }
}
