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
    public synchronized boolean claimMemory(long numBytes) {
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
    public synchronized void releaseMemory(long numBytes) {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        wrapped.releaseMemory(numBytes);
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        releaseMemory(cache);
        if (wrapped.usedMemory() > 0) {
            log.error("CachedMemoryReserver is leaking! Memory left: " + wrapped.usedMemory());
            wrapped.releaseMemory(wrapped.usedMemory());
        }
        wrapped.close();
        closed = true;
    }
}
