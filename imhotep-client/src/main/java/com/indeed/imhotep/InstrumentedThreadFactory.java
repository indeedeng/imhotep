/*
 * Copyright (C) 2015 Indeed Inc.
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

import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ThreadFactory;

public class InstrumentedThreadFactory
    implements Closeable, Instrumentation.Provider, ThreadFactory {

    private static final Logger log = Logger.getLogger(InstrumentedThreadFactory.class);

    private Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    /* Note that Hashtable is used for its thread-safe properties. */
    private final Hashtable<Long, Long> threadToCPUUser  = new Hashtable<Long, Long>();
    private final Hashtable<Long, Long> threadToCPUTotal = new Hashtable<Long, Long>();

    public static class CPUEvent extends Instrumentation.Event {
        protected CPUEvent(String name, long user, long total) {
            super(name);
            getProperties().put(Instrumentation.Keys.CPU_USER,   user);
            getProperties().put(Instrumentation.Keys.CPU_TOTAL, total);
        }
    }

    public static final class PerThreadCPUEvent extends CPUEvent {
        public static final String NAME = PerThreadCPUEvent.class.getSimpleName();
        public PerThreadCPUEvent(long id, long user, long total) {
            super(NAME, user, total);
            getProperties().put(Instrumentation.Keys.THREAD_ID, id);
        }
    }

    public static final class TotalCPUEvent extends CPUEvent {
        public static final String NAME = TotalCPUEvent.class.getSimpleName();
        public TotalCPUEvent(long user, long total) { super(NAME, user, total); }
    }

    private final Vector<Long> ids = new Vector<Long>();

    class InstrumentedThread extends Thread {
        public InstrumentedThread(Runnable runnable) {
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
                if (userTime > 0) InstrumentedThreadFactory.this.threadToCPUUser.put(id, userTime);
                if (cpuTime  > 0) InstrumentedThreadFactory.this.threadToCPUTotal.put(id, cpuTime);
            }
            catch (Exception ex) {
                log.warn("problem while capturing per-thread cpu use", ex);
            }
        }
    }

    public Long cpuUser(ThreadMXBean mxb, long id) {
        final long userTime = mxb.getThreadUserTime(id);
        if (userTime > 0) return userTime;

        final Long result = threadToCPUUser.get(id);
        return result != null ? result : 0;
    }

    public Long cpuTotal(ThreadMXBean mxb, long id) {
        final long totalTime = mxb.getThreadCpuTime(id);
        if (totalTime > 0) return totalTime;

        final Long result = threadToCPUTotal.get(id);
        return result != null ? result : 0;
    }

    public Thread newThread(Runnable runnable) {
        final Thread result = new InstrumentedThread(runnable);
        ids.add(result.getId());
        return result;
    }

    public void addObserver(Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    public void removeObserver(Instrumentation.Observer observer) {
        instrumentation.removeObserver(observer);
    }

    public void close() throws IOException {
        try {
            final ThreadMXBean  mxb       = ManagementFactory.getThreadMXBean();
            final TreeSet<Long> uniqueIds = new TreeSet<Long>(ids);

            long totalCpuTime  = 0;
            long totalUserTime = 0;

            Iterator<Long> it = uniqueIds.iterator();
            while (it.hasNext()) {
                final long id        = it.next();
                final long userTime  = cpuUser(mxb, id);
                final long cpuTime   = cpuTotal(mxb, id);
                totalUserTime       += userTime;
                totalCpuTime        += cpuTime;

                final PerThreadCPUEvent ptcpu = new PerThreadCPUEvent(id, userTime, cpuTime);
                instrumentation.fire(ptcpu);
            }
            final TotalCPUEvent tcpu = new TotalCPUEvent(totalUserTime, totalCpuTime);
            instrumentation.fire(tcpu);
        }
        catch (Exception ex) {
            log.warn("problem while capturing per-thread cpu use", ex);
        }
    }
}
