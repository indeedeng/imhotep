package com.indeed.imhotep.local;

import com.google.common.base.Preconditions;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.FTGSIterator;

import javax.annotation.WillCloseWhenClosed;

/**
 * {@link FTGSIterator} that also has single stat, wraps a {@link FTGAIterator} that has single stat,
 * where the result stat is the group after the bucketing.
 */
public class AggregateBucketFTGSIterator implements FTGSIterator {
    private final double[] buf = new double[1];
    private final FTGAIterator inner;
    private final BucketParams bucketParams;

    public AggregateBucketFTGSIterator(@WillCloseWhenClosed final FTGAIterator inner, final BucketParams bucketParams) {
        Preconditions.checkArgument(inner.getNumStats() == 1);
        this.inner = inner;
        this.bucketParams = bucketParams;
    }

    @Override
    public int getNumStats() {
        return inner.getNumStats();
    }

    @Override
    public void groupStats(final long[] stats, final int offset) {
        inner.groupStats(buf);
        final double value = buf[0];
        stats[offset] = bucketParams.getBucket(value, group());
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
