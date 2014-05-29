package com.indeed.flamdex.fieldcache;

import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.util.mmap.LongArray;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

/**
 * @author jsgroth
 */
public final class MMapLongArrayIntValueLookup implements IntValueLookup {
    private static final Logger LOG = Logger.getLogger(MMapLongArrayIntValueLookup.class);

    private final MMapBuffer buffer;
    private final LongArray longArray;

    public MMapLongArrayIntValueLookup(MMapBuffer buffer, int length) {
        this.buffer = buffer;
        this.longArray = buffer.memory().longArray(0, length);
    }

    @Override
    public long getMin() {
        return Character.MIN_VALUE;
    }

    @Override
    public long getMax() {
        return Character.MAX_VALUE;
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = longArray.get(docIds[i]);
        }
    }

    @Override
    public long memoryUsed() {
        return 0;
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(buffer, LOG);
    }
}
