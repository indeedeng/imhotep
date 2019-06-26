package com.indeed.imhotep.local;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.FTGSIterator;

import javax.annotation.WillCloseWhenClosed;

/**
 * {@link FTGSIterator} that also has single stat, wraps a {@link FTGAIterator} that has single stat,
 * where the result stat is the group after the bucketing.
 */
public class AggregateBucketFTGSIterator implements FTGSIterator {
    private final FTGAIterator inner;
    private final double[] buf = new double[1];
    private final long min;
    private final long max;
    private final long interval;
    private final int numBucketsIncludingGutter;
    private final int lowerGutter;
    private final int upperGutter;

    public AggregateBucketFTGSIterator(@WillCloseWhenClosed final FTGAIterator inner, final long min, final long max, final long interval, final boolean excludeGutters, final boolean withDefault) {
        Preconditions.checkArgument(inner.getNumStats() == 1);
        this.inner = inner;
        this.min = min;
        this.max = max;
        this.interval = interval;
        final int numBuckets = Ints.checkedCast((max - min) / interval);
        this.numBucketsIncludingGutter = (excludeGutters ? 0 : (withDefault ? 1 : 2)) + numBuckets;
        this.lowerGutter = excludeGutters ? 0 : (numBuckets + 1);
        this.upperGutter = excludeGutters ? 0 : (numBuckets + (withDefault ? 1 : 2));
    }

    // Returns [0, numBuckets + 3).
    // 0 means discard, [1, numBuckets] means it's in some normal bucket, [numBucket+1, numBucket+2] are gutters.
    private int getBucketId(final double value) {
        if (value < min) {
            return lowerGutter;
        }
        if (value >= max) {
            return upperGutter;
        }
        final long longValue = (long) value;
        if (value < min) {
            return lowerGutter;
        }
        if (value >= max) {
            return upperGutter;
        }
        final int bucket = (int) ((longValue - min) / interval);
        return bucket + 1;
    }

    public int getResultNumGroups() {
        return getNumGroups() * numBucketsIncludingGutter;
    }

    @Override
    public int getNumStats() {
        return inner.getNumStats();
    }

    @Override
    public void groupStats(final long[] stats, final int offset) {
        inner.groupStats(buf);
        final double value = buf[0];

        final int bucketId = getBucketId(value);
        if (bucketId == 0) {
            stats[offset] = 0;
        } else {
            stats[offset] = ((group() - 1) * (numBucketsIncludingGutter)) + bucketId;
        }
    }

    @Override
    public int getNumGroups() {
        return inner.getNumGroups();
    }

    @Override
    public boolean nextField() {
        return inner.nextField();
    }

    @Override
    public String fieldName() {
        return inner.fieldName();
    }

    @Override
    public boolean fieldIsIntType() {
        return inner.fieldIsIntType();
    }

    @Override
    public boolean nextTerm() {
        return inner.nextTerm();
    }

    @Override
    public long termDocFreq() {
        return inner.termDocFreq();
    }

    @Override
    public long termIntVal() {
        return inner.termIntVal();
    }

    @Override
    public String termStringVal() {
        return inner.termStringVal();
    }

    @Override
    public byte[] termStringBytes() {
        return inner.termStringBytes();
    }

    @Override
    public int termStringLength() {
        return inner.termStringLength();
    }

    @Override
    public boolean nextGroup() {
        return inner.nextGroup();
    }

    @Override
    public int group() {
        return inner.group();
    }

    @Override
    public void close() {
        inner.close();
    }
}
