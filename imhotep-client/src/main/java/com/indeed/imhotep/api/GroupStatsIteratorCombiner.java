package com.indeed.imhotep.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupStatsIteratorCombiner implements GroupStatsIterator {
    public GroupStatsIteratorCombiner( GroupStatsIterator[] stats_ ) {
        this.stats = new ArrayList<GroupStatsIterator>();
        for( GroupStatsIterator stat : stats_ ) {
            if( stat.HasNext() ) {
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
    public boolean HasNext() {
        return stats.size() > 0;
    }

    @Override
    public long Next() {

        long result = 0;
        int count = stats.size();
        int index = 0;
        while( index < count ) {
            result += stats.get(index).Next();
            if( stats.get(index).HasNext() ) {
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
    public void close() throws IOException {

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
