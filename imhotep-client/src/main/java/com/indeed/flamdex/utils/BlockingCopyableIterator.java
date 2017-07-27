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
 package com.indeed.flamdex.utils;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

import javax.annotation.Nonnull;
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

    public BlockingCopyableIterator(final Iterator<E> it, final int numConsumers, final int bufferSize) {
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
    @Nonnull
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
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
