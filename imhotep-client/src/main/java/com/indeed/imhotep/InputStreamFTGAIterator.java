package com.indeed.imhotep;

import com.google.common.annotations.VisibleForTesting;
import com.indeed.imhotep.FTGSBinaryFormat.FieldStat;
import com.indeed.imhotep.api.FTGAIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author jwolfe
 */
public class InputStreamFTGAIterator extends AbstractInputStreamFTGXIterator implements FTGAIterator {
    private final int numStats;
    private final byte[] bytesBuf;
    private final ByteBuffer byteBuffer;

    public InputStreamFTGAIterator(
            final InputStream in,
            @Nullable final FieldStat[] fieldsStats,
            final int numStats,
            final int numGroups
    ) {
        super(in, fieldsStats, numGroups);
        this.numStats = numStats;
        this.bytesBuf = new byte[numStats * 8];
        this.byteBuffer = ByteBuffer.wrap(bytesBuf);
    }

    @VisibleForTesting
    InputStreamFTGAIterator(final InputStream in,
                            final int numStats,
                            final int numGroups) {
        this(in, null, numStats, numGroups);
    }


    @Override
    void readStats() {
        try {
            readBytes(bytesBuf, 0, bytesBuf.length);
        } catch (final IOException e) {
            iteratorStatus = -1;
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getNumStats() {
        return this.numStats;
    }

    @Override
    public void groupStats(double[] stats) {
        for (int i = 0; i < numStats; i++) {
            stats[i] = byteBuffer.getDouble(i * 8);
        }
    }
}
