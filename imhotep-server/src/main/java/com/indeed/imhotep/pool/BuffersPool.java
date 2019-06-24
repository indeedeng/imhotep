package com.indeed.imhotep.pool;

import java.util.ArrayList;
import java.util.List;

// TODO: refactor after task-based processing is implemented
// This the very first version of memory pool class.
// It's created as a temporary solution
// What to do later:
// 1. move BuffersPool into multisession and pass it in constructors of ImhotepLocalSession
//    to share pool across localsessions. (Maybe share it between all multisessions, one pool per daemon)
// 2. Make some kind of resource management, for example have no more that fixed count of buffers in pool.
//    Or no more than fixed total size of buffers.
// 3. Make it thread-local maybe.
public class BuffersPool {
    private List<int[]> intBuffers = new ArrayList<>();
    private List<long[]> longBuffers = new ArrayList<>();

    public synchronized int[] getIntBuffer(final int size, final boolean precise) {
        for (int i = 0; i < intBuffers.size(); i++) {
            final int[] array = intBuffers.get(i);
            if ((precise && (array.length == size)) || (!precise && (array.length >= size))) {
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

    public synchronized void returnIntBuffers(final int[]... buffers) {
        RuntimeException throwable = null;
        for (final int[] buffer : buffers) {
            try {
                returnIntBuffer(buffer);
            } catch (final RuntimeException e) {
                if (throwable == null) {
                    throwable = e;
                } else {
                    throwable.addSuppressed(e);
                }
            }
        }
        if (throwable != null) {
            throw throwable;
        }
    }

    public synchronized long[] getLongBuffer(final int size, final boolean precise) {
        for (int i = 0; i < longBuffers.size(); i++) {
            final long[] array = longBuffers.get(i);
            if ((precise && (array.length == size)) || (!precise && (array.length >= size))) {
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
