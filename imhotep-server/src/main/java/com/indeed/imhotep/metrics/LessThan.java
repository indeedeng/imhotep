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
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class LessThan extends AbstractBinaryOperator {
    private static final Logger log = Logger.getLogger(LessThan.class);

    public LessThan(final IntValueLookup a, final IntValueLookup b) {
        super(a, b);
    }

    @Override
    public long getMin() {
        return 0;
    }

    @Override
    public long getMax() {
        return 1;
    }

    @Override
    protected void combine(final long[] values, final long[] buffer, final int n) {
        for (int i = 0; i < n; i++) {
            values[i] = (values[i] < buffer[i]) ? 1 : 0;
        }
    }
}
