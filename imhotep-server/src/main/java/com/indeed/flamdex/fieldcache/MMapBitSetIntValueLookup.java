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
import com.indeed.flamdex.datastruct.MMapFastBitSet;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * @author jsgroth
 */
public final class MMapBitSetIntValueLookup implements IntValueLookup {
    private static final Logger LOG = Logger.getLogger(MMapBitSetIntValueLookup.class);

    private final MMapFastBitSet bitSet;

    public MMapBitSetIntValueLookup(final MMapFastBitSet bitSet) {
        this.bitSet = bitSet;
    }

    public MMapBitSetIntValueLookup(final Path path, final int length) throws IOException {
        this(new MMapFastBitSet(path, length, FileChannel.MapMode.READ_ONLY));
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
    public void lookup(final int[] docIds, final long[] values, final int n) {
        for (int i = 0; i < n; ++i) {
            values[i] = bitSet.get(docIds[i]) ? 1 : 0;
        }
    }

    @Override
    public long memoryUsed() {
        return 0;
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(bitSet, LOG);
    }
}
