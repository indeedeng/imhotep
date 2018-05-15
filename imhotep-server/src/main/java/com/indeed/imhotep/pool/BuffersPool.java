package com.indeed.imhotep.pool;

import java.util.ArrayList;
import java.util.List;

// This the very first version of memory pool class.
// It's created as a temporary solution
public class BuffersPool {
    private List<int[]> intBuffers = new ArrayList<>();
    private List<long[]> longBuffers = new ArrayList<>();

    public synchronized int[] getIntBuffer(final int size, final boolean precise) {
        for (int i = 0; i < intBuffers.size(); i++) {
            final int[] array = intBuffers.get(i);
            if ((precise && (array.length >= size)) || (!precise && (array.length >= size))) {
                intBuffers.remove(i);
                return array;
            }
        }
        return new int[size];
    }

    public synchronized void returnIntBuffer(final int[] buffer) {
        if (intBuffers.contains(buffer)) {
            throw new IllegalStateException("int buffer returned twice");
        }
        intBuffers.add(buffer);
    }

    public synchronized long[] getLongBuffer(final int size, final boolean precise) {
        for (int i = 0; i < longBuffers.size(); i++) {
            final long[] array = longBuffers.get(i);
            if ((precise && (array.length >= size)) || (!precise && (array.length >= size))) {
                longBuffers.remove(i);
                return array;
            }
        }
        return new long[size];
    }

    public synchronized void returnLongBuffer(final long[] buffer) {
        if (longBuffers.contains(buffer)) {
            throw new IllegalStateException("long buffer returned twice");
        }
        longBuffers.add(buffer);
    }
}
