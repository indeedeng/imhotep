package com.indeed.imhotep.local;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BucketParamsTest {
    @Test
    public void testGutters() {
        final BucketParams bucketParams = new BucketParams(0, 10, 5, false, false);
        assertEquals(7, bucketParams.getResultNumGroups(1));
        assertEquals(14, bucketParams.getResultNumGroups(2));
        assertEquals(0, bucketParams.getBucketIdForAbsent(0));
        assertEquals(0, bucketParams.getBucketIdForAbsent(1));
        assertEquals(0, bucketParams.getBucketIdForAbsent(2));
        assertEquals(1, bucketParams.getBucket(0.0, 1));
        assertEquals(13, bucketParams.getBucket(-10.0, 2));
        assertEquals(14, bucketParams.getBucket(10.0, 2));
        assertEquals(8, bucketParams.getBucket(0.0, 2));
        assertEquals(9, bucketParams.getBucket(2.0, 2));
        assertEquals(10, bucketParams.getBucket(4.0, 2));
        assertEquals(10, bucketParams.getBucket(5.0, 2));
    }

    @Test
    public void testDefaults() {
        final BucketParams bucketParams = new BucketParams(0, 10, 5, false, true);
        assertEquals(6, bucketParams.getResultNumGroups(1));
        assertEquals(12, bucketParams.getResultNumGroups(2));
        assertEquals(0, bucketParams.getBucketIdForAbsent(0));
        assertEquals(6, bucketParams.getBucketIdForAbsent(1));
        assertEquals(12, bucketParams.getBucketIdForAbsent(2));
        assertEquals(1, bucketParams.getBucket(0.0, 1));
        assertEquals(12, bucketParams.getBucket(-10.0, 2));
        assertEquals(12, bucketParams.getBucket(10.0, 2));
        assertEquals(7, bucketParams.getBucket(0.0, 2));
        assertEquals(8, bucketParams.getBucket(2.0, 2));
        assertEquals(9, bucketParams.getBucket(4.0, 2));
        assertEquals(9, bucketParams.getBucket(5.0, 2));
    }

    @Test
    public void testNoGutters() {
        final BucketParams bucketParams = new BucketParams(0, 10, 5, true, false);
        assertEquals(5, bucketParams.getResultNumGroups(1));
        assertEquals(10, bucketParams.getResultNumGroups(2));
        assertEquals(0, bucketParams.getBucketIdForAbsent(0));
        assertEquals(0, bucketParams.getBucketIdForAbsent(1));
        assertEquals(0, bucketParams.getBucketIdForAbsent(2));
        assertEquals(1, bucketParams.getBucket(0.0, 1));
        assertEquals(0, bucketParams.getBucket(-10.0, 2));
        assertEquals(0, bucketParams.getBucket(10.0, 2));
        assertEquals(6, bucketParams.getBucket(0.0, 2));
        assertEquals(7, bucketParams.getBucket(2.0, 2));
        assertEquals(8, bucketParams.getBucket(4.0, 2));
        assertEquals(8, bucketParams.getBucket(5.0, 2));
    }

    @Test
    public void testFloatingPoint() {
        final BucketParams bucketParams = new BucketParams(0, 1, 5, true, false);
        assertEquals(5, bucketParams.getResultNumGroups(1));
        assertEquals(10, bucketParams.getResultNumGroups(2));
        assertEquals(0, bucketParams.getBucketIdForAbsent(0));
        assertEquals(1, bucketParams.getBucket(0.0, 1));
        assertEquals(1, bucketParams.getBucket(0.1, 1));
        assertEquals(2, bucketParams.getBucket(0.2, 1));
        assertEquals(0, bucketParams.getBucket(1.0, 1));
    }
}