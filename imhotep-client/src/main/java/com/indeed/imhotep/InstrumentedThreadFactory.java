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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.ThreadFactory;

public class InstrumentedThreadFactory
    implements Closeable, Instrumentation.Provider, ThreadFactory {

    private static final Logger log = Logger.getLogger(InstrumentedThreadFactory.class);

    private Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    public static final String CPU_USER  = "cpuUserNanos";
    public static final String CPU_TOTAL = "cpuTotalNanos";
    public static final String THREAD_ID = "threadId";

    public static class CPU extends Instrumentation.Event {
        protected CPU(String name, long user, long total) {
            super(name);
            getProperties().put(CPU_USER,   user);
            getProperties().put(CPU_TOTAL, total);
        }
    }

    public static final class PerThreadCPU extends CPU {
        public static final String NAME = PerThreadCPU.class.getSimpleName();
        public PerThreadCPU(long id, long user, long total) {
            super(NAME, user, total);
            getProperties().put(THREAD_ID, id);
        }
    }

    public static final class TotalCPU extends CPU {
        public static final String NAME = TotalCPU.class.getSimpleName();
        public TotalCPU(long user, long total) { super(NAME, user, total); }
    }

    private final ThreadFactory factory;

    private final ArrayList<Long> ids = new ArrayList<Long>();

    public InstrumentedThreadFactory(ThreadFactory factory) {
        this.factory = factory;
    }

    public Thread newThread(Runnable runnable) {
        final Thread result = factory.newThread(runnable);
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
                final long userTime  = mxb.getThreadUserTime(id);
                final long cpuTime   = mxb.getThreadCpuTime(id);
                totalUserTime       += userTime;
                totalCpuTime        += cpuTime;

                final PerThreadCPU ptcpu = new PerThreadCPU(id, userTime, cpuTime);
                instrumentation.fire(ptcpu);
            }
            final TotalCPU tcpu = new TotalCPU(totalUserTime, totalCpuTime);
            instrumentation.fire(tcpu);
        }
        catch (Exception ex) {
            log.warn("problem while capturing per-thread cpu use", ex);
        }
    }
}
