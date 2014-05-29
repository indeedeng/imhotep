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
