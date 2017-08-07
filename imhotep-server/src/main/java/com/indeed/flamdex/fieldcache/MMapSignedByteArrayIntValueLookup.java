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
 package com.indeed.flamdex.fieldcache;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.mmap.ByteArray;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

/**
 * @author jsgroth
 */
public final class MMapSignedByteArrayIntValueLookup implements IntValueLookup {
    private static final Logger LOG = Logger.getLogger(MMapSignedByteArrayIntValueLookup.class);

    private final MMapBuffer buffer;
    private final ByteArray byteArray;
    private final long min;
    private final long max;

    public MMapSignedByteArrayIntValueLookup(final MMapBuffer buffer, final int length, final long min, final long max) {
        this.buffer = buffer;
        this.min = min;
        this.max = max;
        this.byteArray = buffer.memory().byteArray(0, length);
    }

    @Override
    public long getMin() {
        return min;
    }

    @Override
    public long getMax() {
        return max;
    }

    @Override
    public void lookup(final int[] docIds, final long[] values, final int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = byteArray.get(docIds[i]);
        }
    }

    @Override
    public long memoryUsed() {
        return 0;
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(buffer, LOG);
    }
}
