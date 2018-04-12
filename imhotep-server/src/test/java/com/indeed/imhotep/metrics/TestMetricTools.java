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

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.fieldcache.LongArrayIntValueLookup;

public final class TestMetricTools {
    private TestMetricTools() {
    }

    public static IntValueLookup range(final long min, final long max) {
        if (min > max) {
            throw new IllegalArgumentException("min must be not greater than max");
        }
        return new LongArrayIntValueLookup(new long[0], min, max);
    }

    public static boolean isExactRange(final IntValueLookup lookup, final long min, final long max) {
        return (min == lookup.getMin())
                && (lookup.getMin() <= lookup.getMax())
                && (lookup.getMax() == max);
    }

    public static boolean isInRange(final IntValueLookup lookup, final long min, final long max) {
        return (min <= lookup.getMin())
                && (lookup.getMin() <= lookup.getMax())
                && (lookup.getMax() <= max);
    }
}
