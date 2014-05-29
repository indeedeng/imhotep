package com.indeed.flamdex.fieldcache;

import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.MMapFastBitSet;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author jsgroth
 */
public final class MMapBitSetIntValueLookup implements IntValueLookup {
    private static final Logger LOG = Logger.getLogger(MMapBitSetIntValueLookup.class);

    private final MMapFastBitSet bitSet;

    public MMapBitSetIntValueLookup(MMapFastBitSet bitSet) {
        this.bitSet = bitSet;
    }

    public MMapBitSetIntValueLookup(File file, int length) throws IOException {
        this(new MMapFastBitSet(file, length, FileChannel.MapMode.READ_ONLY));
    }

    @Override
    public long getMin() {
        return 0;
    }

    @Override
    public long getMax() {
        return 1;
    }

    @Override
    public void lookup(int[] docIds, long[] values, int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = bitSet.get(docIds[i]) ? 1 : 0;
        }
    }

    @Override
    public long memoryUsed() {
        return 0;
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(bitSet, LOG);
    }
}
