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
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a piece of work that will happen in a single thread
 */
public class ImhotepTask implements Comparable<ImhotepTask> {
    final static ThreadLocal<ImhotepTask> THREAD_LOCAL_TASK = new ThreadLocal<>();

    private static final AtomicLong nextTaskId = new AtomicLong(0);
    private static final Logger log = Logger.getLogger(ImhotepTask.class);
    private final long creationTimestamp;
    private final long taskId;
    final String userName;
    private final String clientName;
    @Nullable private final AbstractImhotepMultiSession session;
    private CountDownLatch waitLock = null;
    private long lastExecutionStartTime = 0;
    private long lastWaitStartTime = 0;
    private long totalExecutionTime = 0;
    private TaskScheduler ownerScheduler = null;


    public static void setup(AbstractImhotepMultiSession session) {
        final ImhotepTask task = new ImhotepTask(session);
        ImhotepTask.THREAD_LOCAL_TASK.set(task);
    }

    public static void setup(String userName, String clientName) {
        final ImhotepTask task = new ImhotepTask(userName, clientName, null);
        ImhotepTask.THREAD_LOCAL_TASK.set(task);
    }

    public static void clear() {
        final ImhotepTask clearingTask = ImhotepTask.THREAD_LOCAL_TASK.get();
        if (clearingTask.getTotalExecutionTime() > TimeUnit.MINUTES.toNanos(1) ) {
            log.warn("Task " + clearingTask.toString() + " took " + TimeUnit.NANOSECONDS.toMillis(clearingTask.getTotalExecutionTime()) + " milli-seconds for execution.");
        }

        ImhotepTask.THREAD_LOCAL_TASK.remove();
    }

    private ImhotepTask(String userName, String clientName, @Nullable AbstractImhotepMultiSession session) {
        this.userName = userName;
        this.clientName = clientName;
        this.taskId = nextTaskId.incrementAndGet();
        creationTimestamp = System.nanoTime();
        this.session = session;
    }

    private ImhotepTask(AbstractImhotepMultiSession session) {
        this(session.getUserName(), session.getClientName(), session);
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
        if(session != null) {
            session.schedulerWaitTimeCallback(schedulerType, waitTime);
        }
        waitLock.countDown();
        lastExecutionStartTime = System.nanoTime();
    }

    void blockAndWait() {
        final CountDownLatch lockToWaitOn;
        synchronized (this) {
            lockToWaitOn = waitLock;
        }
        boolean finishedWaiting = false;
        while (!finishedWaiting) {
            try {
                lockToWaitOn.await();
                finishedWaiting = true;
            } catch(InterruptedException ignored){ }
        }
    }

    /** returns the task resource consumption */
    synchronized long stopped(SchedulerType schedulerType) {
        if(!isRunning()) {
            throw new IllegalStateException("Tried to finish a task that wasn't started");
        }
        long executionTime = System.nanoTime() - lastExecutionStartTime;
        totalExecutionTime += executionTime;
        if(session != null) {
            session.schedulerExecTimeCallback(schedulerType, executionTime);
        }
        lastExecutionStartTime = 0;
        ownerScheduler = null;
        return executionTime;
    }

    @Override
    public String toString() {
        String taskStringVal ="ImhotepTask{" +
                "taskId=" + taskId +
                ", userName='" + userName + '\'' +
                ", clientName='" + clientName + '\'';
        if (session != null) {
            taskStringVal += ", sessionID='" + session.getSessionId() + '\'';
        }
        taskStringVal += '}';

        return taskStringVal;
    }


    /** Used to prioritize older tasks in the TaskScheduler */
    @Override
    public int compareTo(@Nonnull ImhotepTask o) {
        return Longs.compare(creationTimestamp - o.creationTimestamp, 0);
    }

    public boolean isRunning() {
        return lastExecutionStartTime != 0;
    }

    public long getLastExecutionStartTime() {
        return lastExecutionStartTime;
    }

    public long getLastWaitStartTime() {
        return lastWaitStartTime;
    }

    public long getTotalExecutionTime() {
        return totalExecutionTime;
    }
}
