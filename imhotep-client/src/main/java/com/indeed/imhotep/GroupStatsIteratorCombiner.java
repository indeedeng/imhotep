package com.indeed.imhotep;

import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.longs.AbstractLongIterator;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Combine several iterators to one.
 * Result iterator values are sum of corresponding input iterator values.
 *
 * @author aibragimov
 */

class GroupStatsIteratorCombiner extends AbstractLongIterator implements GroupStatsIterator {

    private static final Logger log = Logger.getLogger( GroupStatsIteratorCombiner.class);

    private final List<GroupStatsIterator> stats;
    private final int size;

    GroupStatsIteratorCombiner(final GroupStatsIterator[] stats) {
        int size = 0;
        this.stats = new LinkedList<>();
        for( final GroupStatsIterator stat : stats ) {
            if( stat.hasNext() ) {
                this.stats.add( stat );
                size = Math.max(size, stat.getGroupsCount());
            } else {
                Closeables2.closeQuietly( stat, log );
            }
        }

        this.size = size;
    }

    @Override
    public int getGroupsCount() {
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
        for(final Iterator<GroupStatsIterator> iter = stats.iterator(); iter.hasNext(); ) {
            final GroupStatsIterator stat = iter.next();
            result += stat.nextLong();
            if( !stat.hasNext() ) {
                iter.remove();
            }
        }

        return result;
    }

    @Override
    public void close(){
        Closeables2.closeAll( stats, log );
    }
}
