package com.indeed.imhotep.metrics;

import org.junit.Test;

import static com.indeed.imhotep.metrics.TestMetricTools.isExactRange;
import static com.indeed.imhotep.metrics.TestMetricTools.range;
import static org.junit.Assert.assertTrue;

public final class TestDivision {
    @Test
    public void testMinMax() {
        assertTrue(isExactRange(new Division(range(10, 10), range(5, 5)), 2, 2));
        assertTrue(isExactRange(new Division(range(1, 100), range(1, 100)), 0, 100));
        assertTrue(isExactRange(new Division(range(0, 100), range(0, 100)), 0, 100));
        assertTrue(isExactRange(new Division(range(-10, 10), range(-5, 5)), -10, 10));
        assertTrue(isExactRange(new Division(range(-10, 10), range(0, 0)), 0, 0));
        assertTrue(isExactRange(new Division(range(-10, 10), range(3, 5)), -3, 3));
        assertTrue(isExactRange(new Division(range(0, 0), range(-100, 100)), 0, 0));
        assertTrue(isExactRange(new Division(range(10, 100), range(-100, 100)), -100, 100));
    }
}
