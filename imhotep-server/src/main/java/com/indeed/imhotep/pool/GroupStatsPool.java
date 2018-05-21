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
            throw new IllegalStateException();
        }
        buffersInUse++;
        return buffers.isEmpty() ? new long[numGroups] : buffers.remove(buffers.size() - 1);
    }

    public synchronized void returnBuffer(final long[] partialResult) {
        if (closed) {
            throw new IllegalStateException();
        }
        if (buffersInUse <= 0) {
            throw new IllegalStateException();
        }
        buffersInUse--;
        buffers.add(partialResult);
    }

    public synchronized long[] getTotalResult() {
        closed = true;
        if (buffersInUse != 0) {
            throw new IllegalStateException();
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

        return result;
    }
}
