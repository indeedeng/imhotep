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
 package com.indeed.imhotep;

import com.indeed.util.varexport.Export;
import com.indeed.util.varexport.VarExporter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jsgroth
 */

/**
 * a class for managing large memory allocations to help prevent an ImhotepDaemon from slamming into OutOfMemoryError
 * primary allocation candidates for being managed:
 *   - allocating a new ImhotepLocalSession (specifically the docIdToGroupId array)
 *   - loading a new metric (most of these involve caching an int field of an inverted index)
 */
public final class ImhotepMemoryPool extends MemoryReserver {
    private final long capacityInBytes;

    private final AtomicLong sizeInBytes = new AtomicLong(0);

    public ImhotepMemoryPool(final long capacityInBytes) {
        this.capacityInBytes = capacityInBytes;

        VarExporter.forNamespace(getClass().getSimpleName()).includeInGlobal().export(this, "");
    }

    @Export(name = "used-memory", doc = "claimed memory in bytes")
    public long usedMemory() {
        return sizeInBytes.get();
    }

    @Export(name = "total-memory", doc = "total memory in bytes")
    public long totalMemory() {
        return capacityInBytes;
    }

    public boolean claimMemory(final long numBytes) {
        if (numBytes < 0) {
            return false;
        }
        final long used = sizeInBytes.addAndGet(numBytes);
        if (used > capacityInBytes || used < 0) {
            sizeInBytes.addAndGet(-numBytes);
            return false;
        }
        return true;
    }

    public void releaseMemory(final long numBytes) {
        final long used = sizeInBytes.addAndGet(-numBytes);
        if (used < 0) {
            sizeInBytes.addAndGet(numBytes);
            throw new IllegalArgumentException("trying to free too many bytes: " + numBytes + ", current size: " + (used+numBytes));
        }
    }

    @Override
    public void close() {}
}
