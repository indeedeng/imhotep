package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.longs.AbstractLongIterator;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of GroupStatsIterator over a socket's Inputstream
 *
 * @author aibragimov
 */

class GroupStatsStreamReader extends AbstractLongIterator implements GroupStatsIterator {

    private static Logger log = Logger.getLogger(GroupStatsStreamReader.class);

    private DataInputStream stream;
    private final int count;
    private int index;

    public GroupStatsStreamReader(final InputStream stream, final int count) {
        this.stream = new DataInputStream(stream);
        this.count = count;
        index = 0;
    }

    @Override
    public int getNumGroups() {
        return count;
    }

    @Override
    public boolean hasNext() {
        return stream != null && index < count;
    }

    @Override
    public long nextLong() {
        try {
            index++;
            return stream.readLong();
        } catch ( final IOException e ) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close() {
        Closeables2.closeQuietly( stream, log );
        stream = null;
    }
}
