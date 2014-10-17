/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
