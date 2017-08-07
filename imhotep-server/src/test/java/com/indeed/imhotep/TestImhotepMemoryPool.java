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

import com.google.common.collect.Lists;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

/**
 * @author jsgroth
 */
public class TestImhotepMemoryPool {
    private static final int NUM_THREADS = 100;

    @Test
    public void testConcurrency() throws ExecutionException, InterruptedException, TimeoutException {
        final ImhotepMemoryPool pool = new ImhotepMemoryPool(1024*1024*1024);
        final Random rand = new Random();
        for (int z = 0; z < 100; ++z) {
            final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS, new ThreadFactory() {
                @Override
                public Thread newThread(@Nonnull final Runnable r) {
                    final Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    return t;
                }
            });
            final List<Future<?>> futures = Lists.newArrayList();
            for (int i = 0; i < NUM_THREADS; ++i) {
                futures.add(executor.submit(new Runnable() {
                    long allocated = 0L;

                    @Override
                    public void run() {
                        while (true) {
                            final long l = (rand.nextLong() & Long.MAX_VALUE) % (1024*1024);
                            if (!pool.claimMemory(l)) {
                                break;
                            }
                            allocated += l;
                        }

                        while (allocated > 0L) {
                            final long l = Math.max((rand.nextLong() & Long.MAX_VALUE) % (Math.max(1, allocated / 4)), Math.min(allocated, 1024));
                            pool.releaseMemory(l);
                            allocated -= l;
                        }
                    }
                }));
            }
            try {
                for (final Future<?> future : futures) {
                    future.get(10L, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdown();
            }

            assertEquals(0L, pool.usedMemory());
        }
    }
}
