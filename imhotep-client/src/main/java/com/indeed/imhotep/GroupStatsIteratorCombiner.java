package com.indeed.imhotep;

import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Combine several iterators to one.
 * Result iterator values are sum of corresponding input iterator values.
 *
 * @author aibragimov
 */

class GroupStatsIteratorCombiner implements GroupStatsIterator {

    private static final Logger log = Logger.getLogger( GroupStatsIteratorCombiner.class);

    private final List<GroupStatsIterator> stats;
    private final int size;

    GroupStatsIteratorCombiner(final GroupStatsIterator[] stats) {
        int size = 0;
        this.stats = new ArrayList<>();
        for( final GroupStatsIterator stat : stats ) {
            if( stat.hasNext() ) {
                this.stats.add( stat );
                size = Math.max(size, stat.statSize());
            } else {
                Closeables2.closeQuietly( stat, log );
            }
        }

        this.size = size;
    }

    @Override
    public int statSize() {
        return size;
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
        int index = 0;
        while( index < stats.size() ) {
            result += stats.get(index).nextLong();
            if( stats.get(index).hasNext() ) {
                index++;
            } else {
                // todo : handle deletion in a less hacky way
                stats.set(index, stats.get(stats.size() - 1));
                stats.remove(stats.size() - 1);
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
}
