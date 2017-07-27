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
 package com.indeed.imhotep;

import com.indeed.util.varexport.Export;
import com.indeed.util.varexport.VarExporter;
import org.apache.log4j.Logger;

/**
 * @author jsadun
 */
public class CachedMemoryReserver extends MemoryReserver {
    private static final Logger log = Logger.getLogger(CachedMemoryReserver.class);

    private final MemoryReserver wrapped;
    private final ImhotepMemoryCache cache;

    private boolean closed = false;

    public CachedMemoryReserver(final MemoryReserver wrapped, final ImhotepMemoryCache cache) {
        this.wrapped = wrapped;
        this.cache = cache;

        VarExporter.forNamespace(getClass().getSimpleName()).includeInGlobal().export(this, "");
    }

    @Override
    @Export(name = "used-memory", doc = "claimed memory (not cached) in bytes")
    public synchronized long usedMemory() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        return wrapped.usedMemory() - cachedMemory();
    }

    @Export(name = "cached-memory", doc = "cached memory in bytes")
    public synchronized long cachedMemory() {
        return cache.memoryUsed();
    }

    @Override
    public synchronized long totalMemory() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        return wrapped.totalMemory();
    }

    @Override
    public synchronized boolean claimMemory(final long numBytes) {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        while (!wrapped.claimMemory(numBytes)) {
            final MemoryMeasured value = cache.poll();
            if (value == null) {
                return false;
            } else {
                releaseMemory(value);
            }
        }
        return true;
    }

    @Override
    public synchronized void releaseMemory(final long numBytes) {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        wrapped.releaseMemory(numBytes);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        releaseMemory(cache);
        if (wrapped.usedMemory() > 0) {
            log.error("CachedMemoryReserver is leaking! Memory left: " + wrapped.usedMemory());
            wrapped.releaseMemory(wrapped.usedMemory());
        }
        wrapped.close();
        closed = true;
    }
}
