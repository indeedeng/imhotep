package com.indeed.flamdex.fieldcache;

import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.util.mmap.CharArray;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

/**
 * @author jsgroth
 */
public final class MMapCharArrayIntValueLookup implements IntValueLookup {
    private static final Logger LOG = Logger.getLogger(MMapCharArrayIntValueLookup.class);

    private final MMapBuffer buffer;
    private final CharArray charArray;

    public MMapCharArrayIntValueLookup(MMapBuffer buffer, int length) {
        this.buffer = buffer;
        this.charArray = buffer.memory().charArray(0, length);
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
            values[i] = charArray.get(docIds[i]);
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
