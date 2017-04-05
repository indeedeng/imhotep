package com.indeed.imhotep.api;

public class GroupStatsDummyIterator implements GroupStatsIterator {
    public GroupStatsDummyIterator( long[] stats_ ) {
        stats = stats_;
    }

    @Override
    public boolean HasNext() {
        return stats != null && index < stats.length;
    }

    @Override
    public long Next() {
        return stats[index++];
    }

    @Override
    public void close() {
        stats = null;
        index = 0;
    }

    private int index = 0;
    private long[] stats;
}

