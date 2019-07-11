package com.indeed.imhotep.local;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

public class BucketParams {
    private final long min;
    private final long max;
    private final double doubleMin;
    private final double doubleMax;
    private final long interval;
    private final int numBucketsIncludingGutter;
    private final int lowerGutter;
    private final int upperGutter;
    private final int bucketIdForAbsent;

    public BucketParams(final long min, final long max, final long interval, final boolean excludeGutters, final boolean withDefault) {
        Preconditions.checkArgument(!(excludeGutters && withDefault));
        this.min = min;
        this.max = max;
        this.doubleMin = min;
        this.doubleMax = max;
        this.interval = interval;
        final int numBuckets = Ints.checkedCast((max - min) / interval);
        this.numBucketsIncludingGutter = (excludeGutters ? 0 : (withDefault ? 1 : 2)) + numBuckets;
        this.lowerGutter = excludeGutters ? 0 : (numBuckets + 1);
        this.upperGutter = excludeGutters ? 0 : (numBuckets + (withDefault ? 1 : 2));
        this.bucketIdForAbsent = withDefault ? (numBuckets + 1) : 0;
    }

    private int getBucketId(final double doubleValue) {
        // Comparisons of double vs double to reject doubles that doesn't fit in long
        if (doubleValue < doubleMin) {
            return lowerGutter;
        }
        // > here because doubleValue == doubleMax doesn't imply longValue == max.
        if (doubleValue > doubleMax) {
            return upperGutter;
        }
        // doubleValue >= doubleMin doesn't imply longValue >= min.
        // So we double-check them in long.
        final long longValue = (long) doubleValue;
        if (longValue < min) {
            return lowerGutter;
        }
        if (longValue >= max) {
            return upperGutter;
        }
        final int bucket = (int) ((longValue - min) / interval);
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
