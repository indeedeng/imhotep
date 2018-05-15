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

package com.indeed.imhotep.service;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * An extension of {@link ThreadPoolExecutor} that will block submission of new tasks instead of rejecting them if the queue is full
 * @author ketan
 *
 */
public class BlockingThreadPoolExecutor extends ThreadPoolExecutor {

    private final Semaphore regulator;

    public BlockingThreadPoolExecutor(final int threadPoolSize, final int queueSize, final ThreadFactory threadFactory) {
        super(threadPoolSize, threadPoolSize, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(queueSize), threadFactory);
        this.regulator = new Semaphore(queueSize);
    }

    @Override
    public void execute(final Runnable command) {
        try {
            regulator.acquire();
        } catch (final InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting on regulator semaphore", e);
        }
        super.execute(command);
    }

    @Override
    protected void afterExecute(final Runnable r, final Throwable t) {
        regulator.release();
        super.afterExecute(r, t);
    }
}

