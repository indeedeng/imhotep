package com.indeed.imhotep;

import com.indeed.imhotep.api.GroupStatsIterator;
import it.unimi.dsi.fastutil.longs.AbstractLongIterator;

import java.util.NoSuchElementException;

/**
 * Simple wrapper for GroupStatsIterator over array of longs
 *
 * @author aibragimov
 */

public class GroupStatsDummyIterator extends AbstractLongIterator implements GroupStatsIterator {

    private long[] data;
    private int index;

    public GroupStatsDummyIterator( final long[] data ) {
        this.data = data;
        this.index = 0;
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
    public void close() {
        data = null;
        index = 0;
    }
}