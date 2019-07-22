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

import com.google.common.base.Throwables;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

/**
* @author jsadun
*/
public final class MemoryReservationContext extends MemoryReserver {
    private final MemoryReserver memoryReserver;
    private final long allocationLimit;

    private long reservationSize = 0;
    /* We track two maximum memory values:
        1. Maximum of used memory since creation of class or since last reset.
        2. Global maximum of used memory
     */
    private long currentMaxUsedMemory = 0;
    private long globalMaxUsedMemory = 0;

    private boolean closed = false;

    public MemoryReservationContext(final MemoryReserver memoryReserver) {
        this(memoryReserver, Long.MAX_VALUE);
    }

    public MemoryReservationContext(final MemoryReserver memoryReserver, final long allocationLimit) {
        this.memoryReserver = memoryReserver;
        this.allocationLimit = (allocationLimit > 0) ? allocationLimit : Long.MAX_VALUE;
    }

    public void resetCurrentMaxUsedMemory() {
        currentMaxUsedMemory = reservationSize;
    }

    public long getCurrentMaxUsedMemory() {
        return currentMaxUsedMemory;
    }

    public long getGlobalMaxUsedMemory() {
        return globalMaxUsedMemory;
    }

    public long usedMemory() {
        return reservationSize;
    }

    public long totalMemory() {
        return memoryReserver.totalMemory();
    }

    public synchronized boolean claimMemory(final long numBytes) {
        if (closed) {
            throw new IllegalStateException("cannot allocate memory after reservation context has been closed");
        }
        if ((reservationSize + numBytes) > allocationLimit) {
            return false;
        }
        if (memoryReserver.claimMemory(numBytes)) {
            reservationSize += numBytes;
            updateMax(reservationSize);
            return true;
        }
        return false;
    }

    public synchronized void releaseMemory(final long numBytes) {
        if (closed) {
            throw new IllegalStateException("cannot free memory after reservation context has been closed");
        }
        if (numBytes > reservationSize) {
            throw new IllegalArgumentException("trying to free too many bytes: " + numBytes + ", current size: " + (reservationSize));
        }
        reservationSize -= numBytes;
        try {
            memoryReserver.releaseMemory(numBytes);
        } catch (final Exception e) {
            reservationSize += numBytes;
            throw Throwables.propagate(e);
        }
    }

    public synchronized void hoist(final long numBytes) {
        if (closed) {
            throw new IllegalStateException("cannot hoist memory after reservation context has been closed");
        }
        if (numBytes > reservationSize) {
            throw new IllegalArgumentException("trying to hoist too many bytes: " + numBytes + ", current size: " + reservationSize);
        }
        reservationSize -= numBytes;
    }

    public synchronized void dehoist(final long numBytes) {
        if (closed) {
            throw new IllegalStateException("cannot dehoist memory after reservation context has been closed");
        }
        reservationSize += numBytes;
        updateMax(reservationSize);
    }

    @Override
    public synchronized void close() {
        if (reservationSize > 0) {
            memoryReserver.releaseMemory(reservationSize);
            reservationSize = 0;
        }
        closed = true;
    }

    private void updateMax(final long memorySize) {
        if (memorySize > currentMaxUsedMemory) {
            currentMaxUsedMemory = memorySize;
            if (memorySize > globalMaxUsedMemory) {
                globalMaxUsedMemory = memorySize;
            }
        }
    }
}
