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

import com.google.common.base.Throwables;

/**
* @author jsadun
*/
public final class MemoryReservationContext extends MemoryReserver {
    private final MemoryReserver memoryReserver;

    private long reservationSize = 0;
    private long maxUsedMemory = 0;

    private boolean closed = false;

    public MemoryReservationContext(MemoryReserver memoryReserver) {
        this.memoryReserver = memoryReserver;
    }

    public long maxUsedMemory() {
        return maxUsedMemory;
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
            maxUsedMemory = Math.max(maxUsedMemory, reservationSize);
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
        maxUsedMemory = Math.max(maxUsedMemory, reservationSize);
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
