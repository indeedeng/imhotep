package com.indeed.imhotep.api;

import it.unimi.dsi.fastutil.longs.LongIterator;

public class GroupStatsDummyIterator implements GroupStatsIterator {
    public GroupStatsDummyIterator( LongIterator iterator ) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }

    @Override
    public long nextLong() {
        return iterator.nextLong();
    }

    @Override
    public Long next() {
        return iterator.next();
    }

    @Override
    public int skip( int val ) {
        return iterator.skip( val );
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    public void close() {
        iterator = null;
    }

    private LongIterator iterator;
}

