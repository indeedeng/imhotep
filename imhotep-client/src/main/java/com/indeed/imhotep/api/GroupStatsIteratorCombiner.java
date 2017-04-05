package com.indeed.imhotep.api;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class GroupStatsIteratorCombiner implements GroupStatsIterator {

    private static Logger log = Logger.getLogger( GroupStatsIteratorCombiner.class);

    public GroupStatsIteratorCombiner( final GroupStatsIterator[] stats ) {
        this.stats = new ArrayList<>();
        for( final GroupStatsIterator stat : stats ) {
            if( stat.hasNext() ) {
                this.stats.add( stat );
            } else {
                try {
                    stat.close();
                } catch( final IOException ex ) {
                    log.error("Error while closing GroupStatsIterator");
                }
            }
        }

    }

    @Override
    public boolean hasNext() {
        return !stats.isEmpty();
    }

    @Override
    public long nextLong() throws NoSuchElementException {

        if( stats.isEmpty() ) {
            throw new NoSuchElementException();
        }

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
    public Long next() throws NoSuchElementException {
        return nextLong();
    }

    @Override
    public int skip( final int count ) {
        if( stats.isEmpty() ) {
            return 0;
        }
        int skipCount = stats.get(0).skip(count);
        for( int i = 1; i < stats.size(); i++ ) {
            final int skipped = stats.get( i ).skip( skipCount );
            if( skipped < skipCount ) {
                log.warn("Can't skip " + skipCount + " bytes. Only " + skipped + " bytes skipped.");
                skipCount = skipped;
            }
        }

        return skipCount;
    }

    @Override
    public void remove() {
        for( final GroupStatsIterator stat : stats ) {
            stat.remove();
        }
    }

    @Override
    public void close(){

        for( final GroupStatsIterator stat : stats ) {
            try {
                stat.close();
            } catch( final IOException ex ) {
                log.error("Error while closing GroupStatsIterator");
            }
        }
    }

    private final List<GroupStatsIterator> stats;
}
