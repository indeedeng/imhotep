package com.indeed.flamdex.utils;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @author jsadun
 */
public class BlockingCopyableIterator<E> implements Iterable<E> {
    private final CyclicBarrier barrier;
    private final E[] buffer;
    private int finished;

    public BlockingCopyableIterator(final Iterator<E> it, int numConsumers, int bufferSize) {
        barrier = new CyclicBarrier(numConsumers, new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < buffer.length; i++) {
                    if (it.hasNext()) {
                        buffer[i] = it.next();
                    } else {
                        finished = i;
                        break;
                    }
                }
            }
        });
        // noinspection unchecked
        buffer = (E[]) new Object[bufferSize];
        finished = -1;
    }

    @Override
    public Iterator<E> iterator() {
        return new AbstractIterator<E>() {
            private int myLoc = buffer.length;

            @Override
            protected E computeNext() {
                try {
                    if (myLoc == buffer.length) {
                        barrier.await();
                        myLoc = 0;
                    }
                    if (myLoc == finished) {
                        return endOfData();
                    }
                    final E ret = buffer[myLoc];
                    myLoc++;
                    return ret;
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (BrokenBarrierException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
