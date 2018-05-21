package com.indeed.imhotep.pool;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to manage buffers for getGroupStats.
 *
 */
public class GroupStatsPool {
    private final int numGroups;
    private List<long[]> buffers = new ArrayList<>();
    private int buffersInUse;
    private boolean closed = false;

    public GroupStatsPool(final int numGroups) {
        this.numGroups = numGroups;
    }

    public synchronized long[] getBuffer() {
        if (closed) {
            throw new IllegalStateException("Can't get buffer. Pool is already closed");
        }
        buffersInUse++;
        return buffers.isEmpty() ? new long[numGroups] : buffers.remove(buffers.size() - 1);
    }

    public synchronized void returnBuffer(final long[] partialResult) {
        if (closed) {
            throw new IllegalStateException("Can't return buffer. Pool is already closed");
        }
        if (partialResult.length != numGroups) {
            throw new IllegalStateException("Buffer with unexpected size");
        }
        if (buffersInUse <= 0) {
            throw new IllegalStateException("Returning more buffers than expected");
        }
        buffersInUse--;
        buffers.add(partialResult);
    }

    public synchronized long[] getTotalResult() {
        if (closed) {
            throw new IllegalStateException("Result is already calculated.");
        }
        closed = true;
        if (buffersInUse != 0) {
            throw new IllegalStateException("Some buffers are still in use.");
        }

        if (buffers.isEmpty()) {
            return new long[1];
        }

        final long[] result = buffers.get(0);
        for (int buffer = 1; buffer < buffers.size(); buffer++) {
            final long[] partial = buffers.get(buffer);
            for (int i = 0; i < numGroups; i++) {
                result[i] += partial[i];
            }
        }

        // Free memory now as we don't know GroupStatsPool lifetime.
        buffers = null;

        return result;
    }
}
