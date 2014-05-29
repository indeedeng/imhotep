package com.indeed.imhotep;

import java.util.Arrays;

/**
 * @author jsgroth
 */
public final class BucketStats {
    private final long[] array;
    private final int xBuckets;
    private final int yBuckets;

    /**
     * @param array a group stats array indexed by group
     * @param xBuckets the number of x dimension buckets INCLUDING the 2 gutters
     * @param yBuckets the number of y dimension buckets INCLUDING the 2 gutters
     */
    public BucketStats(long[] array, int xBuckets, int yBuckets) {
        this.xBuckets = xBuckets;
        this.yBuckets = yBuckets;

        final int requiredArrayLen = xBuckets * yBuckets + 1;
        this.array = array.length < requiredArrayLen ? Arrays.copyOf(array, requiredArrayLen) : array;
    }

    public final long get(final int x, final int y) {
        return array[(y + 1) * xBuckets + x + 2];
    }

    public final long getXUnderflow(final int y) {
        return array[(y + 1) * xBuckets + 1];
    }

    public final long getXOverflow(final int y) {
        return array[(y + 2) * xBuckets];
    }

    public final long getYUnderflow(final int x) {
        return array[x + 2];
    }

    public final long getYOverflow(final int x) {
        return array[(yBuckets - 1) * xBuckets + x + 2];
    }

    public final long getXYUnderflow() {
        return array[1];
    }

    public final long getXYOverflow() {
        return array[xBuckets * yBuckets];
    }

    public final long getXUnderflowYOverflow() {
        return array[(yBuckets - 1) * xBuckets + 1];
    }

    public final long getXOverflowYUnderflow() {
        return array[xBuckets];
    }

    public final long[] asArray() {
        return array;
    }
}
