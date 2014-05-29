package com.indeed.imhotep;

import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author jsgroth
 */
public class TestImhotepMemoryPool extends TestCase {
    private static final int NUM_THREADS = 100;

    @Test
    public void testConcurrency() throws ExecutionException, InterruptedException, TimeoutException {
        final ImhotepMemoryPool pool = new ImhotepMemoryPool(1024*1024*1024);
        final Random rand = new Random();
        for (int z = 0; z < 100; ++z) {
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    return t;
                }
            });
            List<Future<?>> futures = Lists.newArrayList();
            for (int i = 0; i < NUM_THREADS; ++i) {
                futures.add(executor.submit(new Runnable() {
                    long allocated = 0L;

                    @Override
                    public void run() {
                        while (true) {
                            long l = (rand.nextLong() & Long.MAX_VALUE) % (1024*1024);
                            if (!pool.claimMemory(l)) {
                                break;
                            }
                            allocated += l;
                        }

                        while (allocated > 0L) {
                            long l = Math.max((rand.nextLong() & Long.MAX_VALUE) % (Math.max(1, allocated / 4)), Math.min(allocated, 1024));
                            pool.releaseMemory(l);
                            allocated -= l;
                        }
                    }
                }));
            }
            try {
                for (Future<?> future : futures) {
                    future.get(10L, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdown();
            }

            assertEquals(0L, pool.usedMemory());
        }
    }
}
