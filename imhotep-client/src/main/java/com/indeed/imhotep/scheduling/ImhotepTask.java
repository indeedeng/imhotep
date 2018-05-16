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

package com.indeed.imhotep.scheduling;

import com.google.common.primitives.Longs;
import com.indeed.imhotep.AbstractImhotepMultiSession;

import javax.annotation.Nonnull;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a piece of work that will happen in a single thread
 */
public class ImhotepTask implements Comparable<ImhotepTask> {
    static ThreadLocal<ImhotepTask> THREAD_LOCAL_TASK = new ThreadLocal<>();

    private static AtomicLong nextTaskId = new AtomicLong(0);
    private final long creationTimestamp;
    private final long taskId;
    final String userName;
    private final String clientName;
    private final AbstractImhotepMultiSession session;
    private CountDownLatch waitLock = null;
    private long lastExecutionStartTime = 0;
    private long lastWaitStartTime = 0;
    private TaskScheduler ownerScheduler = null;

    public static void setup(AbstractImhotepMultiSession session) {
        final ImhotepTask task = new ImhotepTask(session);
        ImhotepTask.THREAD_LOCAL_TASK.set(task);
    }

    public static void clear() {
        ImhotepTask.THREAD_LOCAL_TASK.remove();
    }

    private ImhotepTask(AbstractImhotepMultiSession session) {
        this.userName = session.getUserName();
        this.clientName = session.getClientName();
        this.taskId = nextTaskId.incrementAndGet();
        creationTimestamp = System.nanoTime();
        this.session = session;
    }

    synchronized void preExecInitialize(TaskScheduler newOwnerScheduler) {
        if(ownerScheduler != null) {
            throw new RuntimeException("Tried to lock an already locked Task");
        }
        ownerScheduler = newOwnerScheduler;
        lastWaitStartTime = System.nanoTime();
        waitLock = new CountDownLatch(1);
    }

    synchronized void markRunnable(SchedulerType schedulerType) {
        if(waitLock == null) {
            throw new IllegalStateException("Tried to schedule task that is not startable " + toString());
        }
        long waitTime = System.nanoTime() - lastWaitStartTime;
        session.schedulerWaitTimeCallback(schedulerType, waitTime);
        waitLock.countDown();
        lastExecutionStartTime = System.nanoTime();
    }

    void blockAndWait() {
        final CountDownLatch lockToWaitOn;
        synchronized (this) {
            lockToWaitOn = waitLock;
        }
        boolean finished = false;
        while (!finished) {
            try {
                lockToWaitOn.await();
                finished = true;
            } catch(InterruptedException ignored){ }
        }
    }

    /** returns the task resource consumption */
    synchronized long stopped(SchedulerType schedulerType) {
        if(!isRunning()) {
            throw new IllegalStateException("Tried to finish a task that wasn't started");
        }
        long executionTime = System.nanoTime() - lastExecutionStartTime;
        session.schedulerExecTimeCallback(schedulerType, executionTime);
        lastExecutionStartTime = 0;
        ownerScheduler = null;
        return executionTime;
    }

    @Override
    public String toString() {
        return "ImhotepTask{" +
                "taskId=" + taskId +
                ", userName='" + userName + '\'' +
                ", clientName='" + clientName + '\'' +
                '}';
    }


    /** Used to prioritize older tasks in the TaskScheduler */
    @Override
    public int compareTo(@Nonnull ImhotepTask o) {
        return Longs.compare(creationTimestamp, o.creationTimestamp);
    }

    public boolean isRunning() {
        return lastExecutionStartTime != 0;
    }

    public long getLastExecutionStartTime() {
        return lastExecutionStartTime;
    }
}
