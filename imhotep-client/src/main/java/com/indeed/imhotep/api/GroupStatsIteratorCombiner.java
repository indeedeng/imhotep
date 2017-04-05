package com.indeed.imhotep.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupStatsIteratorCombiner implements GroupStatsIterator {
    public GroupStatsIteratorCombiner( GroupStatsIterator[] stats_ ) {
        this.stats = new ArrayList<GroupStatsIterator>();
        for( GroupStatsIterator stat : stats_ ) {
            if( stat.hasNext() ) {
                this.stats.add( stat );
            } else {
                try {
                    stat.close();
                } catch( IOException ex ) {
                    //log
                }
            }
        }

    }

    @Override
    public boolean hasNext() {
        return stats.size() > 0;
    }

    @Override
    public long nextLong() {

        long result = 0;
        int count = stats.size();
        int index = 0;
        while( index < count ) {
            result += stats.get(index).nextLong();
            if( stats.get(index).hasNext() ) {
                index++;
            } else {
                // delete later???
                stats.set(index, stats.get(index));
                stats.remove( count - 1 );
                count--;
            }
        }

        return result;
    }

    @Override
    public Long next() {
        return nextLong();
    }

    @Override
    public int skip( int skipCount ) {
        if( stats.size() == 0 ) {
            return 0;
        }
        skipCount = stats.get(0).skip(skipCount);
        for( int i = 1; i < stats.size(); i++ ) {
            int skipped = stats.get( i ).skip( skipCount );
            if( skipped < skipCount ) {
                //log
                skipCount = skipped;
            }
        }

        return skipCount;
    }

    @Override
    public void remove() {
        for( GroupStatsIterator stat : stats ) {
            stat.remove();
        }
    }

    @Override
    public void close(){

        for( GroupStatsIterator stat : stats ) {
            try {
                stat.close();
            } catch( IOException ex ) {
                //log
            }
        }
    }

    private final List<GroupStatsIterator> stats;
}
