package com.indeed.imhotep.local;

import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;

public class BucketParams {
    private final double min;
    private final double max;
    private final double interval;
    private final int numBuckets;
    private final int numBucketsIncludingGutter;
    private final int lowerGutter;
    private final int upperGutter;
    private final int bucketIdForAbsent;

    public BucketParams(final double min, final double max, final int numBuckets, final boolean excludeGutters, final boolean withDefault) {
        Preconditions.checkArgument(!(excludeGutters && withDefault));
        this.min = min;
        this.max = max;
        this.interval = (max - min) / numBuckets;
        this.numBuckets = numBuckets;
        this.numBucketsIncludingGutter = (excludeGutters ? 0 : (withDefault ? 1 : 2)) + numBuckets;
        this.lowerGutter = excludeGutters ? 0 : (numBuckets + 1);
        this.upperGutter = excludeGutters ? 0 : (numBuckets + (withDefault ? 1 : 2));
        this.bucketIdForAbsent = withDefault ? (numBuckets + 1) : 0;
    }

    private int getBucketId(final double doubleValue) {
        if (doubleValue < min) {
            return lowerGutter;
        }
        if (doubleValue >= max) {
            return upperGutter;
        }
        final int bucket = (int) Math.floor((doubleValue - min) / interval);
        // It's not guaranteed that (Math.nextDown(max) - min) / interval < numBuckets.
        if (bucket >= numBuckets) {
            return upperGutter;
        }
        return bucket + 1;
    }

    public int getResultNumGroups(final int originalNumGroups) {
        return Ints.checkedCast((long) originalNumGroups * numBucketsIncludingGutter);
    }

    public int getBucketIdForAbsent(final int originalGroup) {
        if ((bucketIdForAbsent == 0) || (originalGroup == 0)) {
            return 0;
        } else {
            return ((originalGroup - 1) * numBucketsIncludingGutter) + bucketIdForAbsent;
        }
    }

    public int getBucket(final double stat, final int originalGroup) {
        final int bucketId = getBucketId(stat);
        if ((bucketId == 0) || (originalGroup == 0)) {
            return 0;
        } else {
            return ((originalGroup - 1) * (numBucketsIncludingGutter)) + bucketId;
        }
    }
}
