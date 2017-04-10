package com.indeed.imhotep;

import com.google.common.primitives.Longs;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

/**
 * Implementation of GroupStatsIterator over a socket's Inputstream
 *
 * @author aibragimov
 */

class GroupStatsStreamReader implements GroupStatsIterator {

    private static Logger log = Logger.getLogger(GroupStatsStreamReader.class);
    private final byte[] buffer = new byte[Long.BYTES];

    private InputStream stream;
    private final int count;
    private int index;

    public GroupStatsStreamReader(final InputStream stream, final int count) {
        this.stream = stream;
        this.count = count;
        index = 0;
    }

    @Override
    public int statSize() {
        return count;
    }

    @Override
    public boolean hasNext() {
        return stream != null && index < count;
    }

    @Override
    public Long next() {
        return nextLong();
    }

    @Override
    public long nextLong() {
        int pos = 0;
        try {
            while( pos < Long.BYTES ) {
                final int readCount = stream.read(buffer, pos, Long.BYTES - pos);
                if (readCount == -1) {
                    // end of stream
                    throw new IOException();
                }
                pos += readCount;
            }
        } catch ( IOException e ) {
            log.error(e);
            throw new NoSuchElementException();
        }

        index++;
        return Longs.fromByteArray(buffer);
    }

    @Override
    public int skip(final int value) {
        final long skipped;
        try {
            skipped = stream.skip(value * Long.BYTES);
        } catch ( final IOException e ) {
            log.error(e);
            throw new NoSuchElementException();
        }
        if( skipped % Long.BYTES != 0 ) {
            log.error("GroupStatsStreamReader: unexpected bytes count in stream");
            throw new NoSuchElementException();
        }
        return (int) skipped / Long.BYTES;
    }

    @Override
    public void close() {
        Closeables2.closeQuietly( stream, log );
        stream = null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
