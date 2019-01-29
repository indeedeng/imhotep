package com.indeed.imhotep.matcher;

import com.google.common.base.Charsets;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class StringMatcherUtilTest {
    @Test
    public void testKMPTable() {
        // Examples from https://en.wikipedia.org/wiki/Knuth–Morris–Pratt_algorithm
        assertArrayEquals(
                new int[]{-1, 0, 0, 0, -1, 0, 2, 0},
                StringMatcherUtil.buildKMPTable("ABCDABD".getBytes(Charsets.UTF_8))
        );
        assertArrayEquals(
                new int[]{-1, 0, -1, 1, -1, 0, -1, 3, 2, 0},
                StringMatcherUtil.buildKMPTable("ABACABABC".getBytes(Charsets.UTF_8))
        );
        assertArrayEquals(
                new int[]{-1, 0, -1, 1, -1, 0, -1, 3, -1, 3},
                StringMatcherUtil.buildKMPTable("ABACABABA".getBytes(Charsets.UTF_8))
        );
        assertArrayEquals(
                new int[]{-1, 0, 0, 0, 0, 0, 0, -1, 0, 2, 0, 0, 0, 0, 0, -1, 0, 0, 3, 0, 0, 0, 0, 0, 0},
                StringMatcherUtil.buildKMPTable("PARTICIPATE IN PARACHUTE".getBytes(Charsets.UTF_8))
        );
    }
}