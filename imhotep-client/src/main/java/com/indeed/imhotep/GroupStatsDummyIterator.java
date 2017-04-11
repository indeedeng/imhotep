package com.indeed.imhotep;

import com.indeed.imhotep.api.GroupStatsIterator;

import java.util.NoSuchElementException;

/**
 * Simple wrapper for GroupStatsIterator over array of longs
 *
 * @author aibragimov
 */

public class GroupStatsDummyIterator implements GroupStatsIterator {

    private long[] data;
    private int index;

    public GroupStatsDummyIterator( final long[] data ) {
        this.data = data;
    }

    @Override
    public int getGroupsCount() {
        return data.length;
    }

    @Override
    public boolean hasNext() {
        return data != null && index < data.length;
    }

    @Override
    public long nextLong() {
        if( index >= data.length ) {
            throw new NoSuchElementException();
        }
        return data[index++];
    }

    @Override
    public Long next() {
        return nextLong();
    }

    @Override
    public int skip( final int val ) {
        final int newIndex = Math.min( index + val, data.length );
        final int skipped = newIndex - index;
        index = newIndex;
        return skipped;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public void close() {
        data = null;
        index = 0;
    }
}