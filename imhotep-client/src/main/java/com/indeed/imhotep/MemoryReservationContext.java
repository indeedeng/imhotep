package com.indeed.imhotep;

import com.google.common.base.Throwables;

/**
* @author jsadun
*/
public final class MemoryReservationContext extends MemoryReserver {
    private final MemoryReserver memoryReserver;

    private long reservationSize = 0;

    private boolean closed = false;

    public MemoryReservationContext(MemoryReserver memoryReserver) {
        this.memoryReserver = memoryReserver;
    }

    public long usedMemory() {
        return reservationSize;
    }

    public long totalMemory() {
        return memoryReserver.totalMemory();
    }

    public synchronized boolean claimMemory(long numBytes) {
        if (closed) throw new IllegalStateException("cannot allocate memory after reservation context has been closed");
        if (memoryReserver.claimMemory(numBytes)) {
            reservationSize += numBytes;
            return true;
        }
        return false;
    }

    public synchronized void releaseMemory(long numBytes) {
        if (closed) throw new IllegalStateException("cannot free memory after reservation context has been closed");
        if (numBytes > reservationSize) {
            throw new IllegalArgumentException("trying to free too many bytes: " + numBytes + ", current size: " + (reservationSize));
        }
        reservationSize -= numBytes;
        try {
            memoryReserver.releaseMemory(numBytes);
        } catch (Exception e) {
            reservationSize += numBytes;
            throw Throwables.propagate(e);
        }
    }

    public synchronized void hoist(long numBytes) {
        if (closed) throw new IllegalStateException("cannot hoist memory after reservation context has been closed");
        if (numBytes > reservationSize) {
            throw new IllegalArgumentException("trying to hoist too many bytes: " + numBytes + ", current size: " + reservationSize);
        }
        reservationSize -= numBytes;
    }

    public synchronized void dehoist(long numBytes) {
        if (closed) throw new IllegalStateException("cannot dehoist memory after reservation context has been closed");
        reservationSize += numBytes;
    }

    @Override
    public synchronized void close() {
        if (reservationSize > 0) {
            memoryReserver.releaseMemory(reservationSize);
            reservationSize = 0;
        }
        closed = true;
    }
}
