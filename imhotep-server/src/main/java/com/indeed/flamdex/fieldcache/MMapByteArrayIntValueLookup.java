package com.indeed.flamdex.fieldcache;

import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.util.mmap.ByteArray;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

/**
 * @author jsgroth
 */
public final class MMapByteArrayIntValueLookup implements IntValueLookup {
    private static final Logger LOG = Logger.getLogger(MMapByteArrayIntValueLookup.class);

    private final MMapBuffer buffer;
    private final ByteArray byteArray;

    public MMapByteArrayIntValueLookup(MMapBuffer buffer, int length) {
        this.buffer = buffer;
        this.byteArray = buffer.memory().byteArray(0, length);
    }

    @Override
    public long getMin() {
        return 0;
    }

    @Override
    public long getMax() {
        return 255;
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = byteArray.get(docIds[i]) & 0xFF;
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
