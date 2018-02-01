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
 package com.indeed.imhotep.metrics;

import com.indeed.flamdex.api.IntValueLookup;

/**
 * @author jsgroth
 */
public class Modulus extends AbstractBinaryOperator {
    public Modulus(final IntValueLookup a, final IntValueLookup b) {
        super(a, b);
    }

    // This calculation of min and max is not accurate
    // but real [min, max] range is inside returned [min, max] range
    //
    // For example, if 'a' has range [100, 200] and 'b' has range [1000, 10000]
    // our code will return range [0, 9999] but correct answer is [100, 200]
    // To calculate correct answer we need to check if all result values are possible,
    // which is kinda tricky, so we use simplified calculation.
    //
    // In Java '%' is defined to be consistent with '/' operator
    // and '/' operator rounds towards zero.
    // Example from Java SE 8 spec (chapter 15.17.3)
    // 5 % 3 == 2
    // 5 % (-3) == 2
    // (-5) % 3 == -2
    // (-5) % (-3) == -2
    // Value of A % B is in range [0, |B|-1] for positive A
    // Value of A % B is in range [-(|B|-1), 0] for negative A
    @Override
    public long getMin() {
        if (a.getMin() < 0) {
            // result can be negative
            return -(Math.max(Math.abs(b.getMin()), Math.abs(b.getMax())) - 1);
        } else {
            // result is always positive or zero.
            return 0;
        }
    }

    @Override
    public long getMax() {
        if (a.getMax() > 0 ) {
            // result can be positive
            return Math.max(Math.abs(b.getMin()), Math.abs(b.getMax())) - 1;
        } else {
            // result can be regative or zero.
            return 0;
        }
     }

    @Override
    protected void combine(final long[] values, final long[] buffer, final int n) {
        for (int i = 0; i < n; ++i) {
            values[i] %= buffer[i];
        }
    }
}
