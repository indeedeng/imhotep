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

import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Hashtable;
import java.util.concurrent.ThreadFactory;

public class InstrumentedThreadFactory
    implements Closeable, Instrumentation.Provider, ThreadFactory {

    private static final Logger log = Logger.getLogger(InstrumentedThreadFactory.class);

    private final Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    /* Note that Hashtable is used for its thread-safe properties. */
    private final Hashtable<Long, Long> threadToCPUUser  = new Hashtable<>();
    private final Hashtable<Long, Long> threadToCPUTotal = new Hashtable<>();

    private final LongRBTreeSet ids = new LongRBTreeSet();

    public static class CPUEvent extends Instrumentation.Event {
        protected CPUEvent(final String name, final long user, final long total) {
            super(name);
            getProperties().put(Instrumentation.Keys.CPU_USER,  user);
            getProperties().put(Instrumentation.Keys.CPU_TOTAL, total);
        }
    }

    public static final class PerThreadCPUEvent extends CPUEvent {
        public static final String NAME = PerThreadCPUEvent.class.getSimpleName();
        public PerThreadCPUEvent(final long id, final long user, final long total) {
            super(NAME, user, total);
            getProperties().put(Instrumentation.Keys.THREAD_ID, id);
        }
    }

    public static final class TotalCPUEvent extends CPUEvent {
        public static final String NAME = TotalCPUEvent.class.getSimpleName();
        public TotalCPUEvent(final long user, final long total, final long threads) {
            super(NAME, user, total);
            getProperties().put(Instrumentation.Keys.TOTAL_THREADS, threads);
        }
    }

    class InstrumentedThread extends Thread {
        public InstrumentedThread(final Runnable runnable) {
            super(runnable);
        }

        @Override public void interrupt() {
            snapshot();
            super.interrupt();
        }

        @Override public void run() {
            try {
                super.run();
            }
            finally {
                snapshot();
            }
        }

        private void snapshot() {
            try {
                final ThreadMXBean mxb      = ManagementFactory.getThreadMXBean();
                final long         id       = getId();
                final long         userTime = mxb.getThreadUserTime(id);
                final long         cpuTime  = mxb.getThreadCpuTime(id);
                if (userTime > 0) {
                    InstrumentedThreadFactory.this.threadToCPUUser.put(id, userTime);
                }
                if (cpuTime  > 0) {
                    InstrumentedThreadFactory.this.threadToCPUTotal.put(id, cpuTime);
                }
            }
            catch (final Exception ex) {
                log.warn("problem while capturing per-thread cpu use", ex);
            }
        }
    }

    public Long cpuUser(final ThreadMXBean mxb, final long id) {
        final long userTime = mxb.getThreadUserTime(id);
        if (userTime > 0) {
            return userTime;
        }

        final Long result = threadToCPUUser.get(id);
        return result != null ? result : 0;
    }

    public Long cpuTotal(final ThreadMXBean mxb, final long id) {
        final long totalTime = mxb.getThreadCpuTime(id);
        if (totalTime > 0) {
            return totalTime;
        }

        final Long result = threadToCPUTotal.get(id);
        return result != null ? result : 0;
    }

    @Override
    public Thread newThread(@Nonnull final Runnable runnable) {
        final Thread result = new InstrumentedThread(runnable);
        synchronized(ids) {
            ids.add(result.getId());
        }
        return result;
    }

    public void addObserver(final Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    public void removeObserver(final Instrumentation.Observer observer) {
        instrumentation.removeObserver(observer);
    }

    public static class PerformanceStats {
        public final long cpuTotalTime;
        public final long cpuUserTime;
        public final long threadCount;

        public PerformanceStats(long cpuTotalTime, long cpuUserTime, long threadCount) {
            this.cpuTotalTime = cpuTotalTime;
            this.cpuUserTime = cpuUserTime;
            this.threadCount = threadCount;
        }
    }

    @Nullable
    public PerformanceStats getPerformanceStats() {
        try {
            final ThreadMXBean mxb = ManagementFactory.getThreadMXBean();
            long totalCpuTime  = 0;
            long totalUserTime = 0;

            synchronized(ids) {
                final LongBidirectionalIterator it = ids.iterator();
                while (it.hasNext()) {
                    final long id        = it.next();
                    final long userTime  = cpuUser(mxb, id);
                    final long cpuTime   = cpuTotal(mxb, id);
                    totalUserTime       += userTime;
                    totalCpuTime        += cpuTime;
                }
            }
            return new PerformanceStats(totalCpuTime, totalUserTime, ids.size());
        }
        catch (final Exception ex) {
            log.warn("problem while capturing per-thread cpu use", ex);
            return null;
        }
    }

    public void close() throws IOException {
        try {
            // Per thread CPU event generation disabled as it becomes too spammy
//            final ThreadMXBean mxb = ManagementFactory.getThreadMXBean();
//            long totalCpuTime  = 0;
//            long totalUserTime = 0;
//
//            synchronized(ids) {
//                final LongBidirectionalIterator it = ids.iterator();
//                while (it.hasNext()) {
//                    final long id        = it.next();
//                    final long userTime  = cpuUser(mxb, id);
//                    final long cpuTime   = cpuTotal(mxb, id);
//                    totalUserTime       += userTime;
//                    totalCpuTime        += cpuTime;
//
//                    final PerThreadCPUEvent ptcpu = new PerThreadCPUEvent(id, userTime, cpuTime);
//                    instrumentation.fire(ptcpu);
//                }
//            }
            final PerformanceStats totalStats = getPerformanceStats();
            if(totalStats != null) {
                final TotalCPUEvent tcpu =
                        new TotalCPUEvent(totalStats.cpuUserTime, totalStats.cpuTotalTime, ids.size());
                instrumentation.fire(tcpu);
            }
        }
        catch (final Exception ex) {
            log.warn("problem while capturing per-thread cpu use", ex);
        }
    }
}
