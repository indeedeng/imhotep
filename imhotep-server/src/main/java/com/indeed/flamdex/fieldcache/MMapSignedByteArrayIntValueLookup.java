package com.indeed.flamdex.fieldcache;

import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.util.mmap.ByteArray;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

/**
 * @author jsgroth
 */
public final class MMapSignedByteArrayIntValueLookup implements IntValueLookup {
    private static final Logger LOG = Logger.getLogger(MMapSignedByteArrayIntValueLookup.class);

    private final MMapBuffer buffer;
    private final ByteArray byteArray;

    public MMapSignedByteArrayIntValueLookup(MMapBuffer buffer, int length) {
        this.buffer = buffer;
        this.byteArray = buffer.memory().byteArray(0, length);
    }

    @Override
    public long getMin() {
        return Byte.MIN_VALUE;
    }

    @Override
    public long getMax() {
        return Byte.MAX_VALUE;
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = byteArray.get(docIds[i]);
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
