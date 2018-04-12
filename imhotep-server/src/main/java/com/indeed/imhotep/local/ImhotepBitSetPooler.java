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
 package com.indeed.imhotep.local;

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;
import com.indeed.imhotep.MemoryReserver;

/**
 * @author jsgroth
 */
public class ImhotepBitSetPooler implements FastBitSetPooler {
    private final MemoryReserver memory;

    public ImhotepBitSetPooler(final MemoryReserver memory) {
        this.memory = memory;
    }

    @Override
    public FastBitSet create(final int size) throws FlamdexOutOfMemoryException {
        if (!memory.claimMemory(memoryUsage(size))) {
            throw new FlamdexOutOfMemoryException();
        }
        return new FastBitSet(size);
    }

    @Override
    public void release(final long bytes) {
        memory.releaseMemory(bytes);
    }

    private static long memoryUsage(final int size) {
        return 8L * ((size + 64) >> 6);
    }
}
