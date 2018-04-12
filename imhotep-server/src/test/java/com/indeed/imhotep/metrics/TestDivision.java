/*
 * Copyright (C) 2018 Indeed Inc.
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
