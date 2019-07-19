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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.AbstractImhotepSession;
import com.indeed.imhotep.RequestContext;
import com.indeed.imhotep.SlotTiming;
import com.indeed.imhotep.exceptions.InvalidSessionException;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a piece of work that will happen in a single thread.
 *
 * Synchronizations on this object are for avoiding race between initialize task vs start/finish task.
 */
public class ImhotepTask implements Comparable<ImhotepTask> {
    private static final Logger LOGGER = Logger.getLogger(ImhotepTask.class);

    public static final ThreadLocal<ImhotepTask> THREAD_LOCAL_TASK = new ThreadLocal<>();
    private static final AtomicLong nextTaskId = new AtomicLong(0);

    private final long creationTimestamp;
    private final long taskId;
    final String userName;
    private final String clientName;
    final byte priority;
    private final Thread taskThread;
    @Nullable private final RequestContext requestContext;
    @Nullable private final String dataset;
    @Nullable private final String shardName;
    @Nullable private final Integer numDocs;
    @Nullable private final AbstractImhotepMultiSession session;
    @Nullable private AbstractImhotepSession innerSession;
    private CountDownLatch waitLock = null;
    private volatile long lastExecutionStartTime = 0;
    private volatile long lastWaitStartTime = 0;
    private volatile long totalExecutionTime = 0;
    private TaskScheduler ownerScheduler = null;
    private final Object executionTimeStatsLock = new Object(); // Lock for changing lastExecutionStartTime, totalExecutionTime, and nextYieldTime atomically

    @Nullable
    private SchedulerCallback schedulerExecTimeCallback;
    @Nullable
    private SchedulerCallback schedulerWaitTimeCallback;

    boolean cancelled;
    private volatile long nextYieldTime = 0;

    public static void setup(final String userName, final String clientName, final byte priority, final SlotTiming slotTiming) {
        setup(userName, clientName, priority, null, null, null, slotTiming);
    }

    public static void setup(AbstractImhotepMultiSession session) {
        final ImhotepTask task = new ImhotepTask(session);
        ImhotepTask.THREAD_LOCAL_TASK.set(task);
    }

    public static boolean hasTask() {
        return ImhotepTask.THREAD_LOCAL_TASK.get() != null;
    }

    public static void setup(
            final String userName,
            final String clientName,
            final byte priority,
            @Nullable final String dataset,
            @Nullable final String shardName,
            @Nullable final Integer numDocs,
            @Nonnull final SlotTiming slotTiming
    ) {
        final ImhotepTask task = new ImhotepTask(userName, clientName, priority, null, dataset, shardName, numDocs,
                (schedulerType, execTime) -> slotTiming.schedulerExecTimeCallback(schedulerType, execTime),
                (schedulerType, waitTime) -> slotTiming.schedulerWaitTimeCallback(schedulerType, waitTime));
        ImhotepTask.THREAD_LOCAL_TASK.set(task);
    }

    public static void registerInnerSession(final AbstractImhotepSession innerSession) {
        final ImhotepTask task = ImhotepTask.THREAD_LOCAL_TASK.get();
        task.innerSession = innerSession;
    }

    public static void clear() {
        final ImhotepTask clearingTask = ImhotepTask.THREAD_LOCAL_TASK.get();
        if (clearingTask.getTotalExecutionTime() > TimeUnit.MINUTES.toNanos(1) ) {
            LOGGER.warn("Task " + clearingTask.toString() + " took " + TimeUnit.NANOSECONDS.toMillis(clearingTask.getTotalExecutionTime()) + " milli-seconds for execution.");
        }

        ImhotepTask.THREAD_LOCAL_TASK.remove();
    }

    private ImhotepTask(
            final String userName,
            final String clientName,
            final byte priority,
            @Nullable final AbstractImhotepMultiSession session,
            @Nullable final String dataset,
            @Nullable final String shardName,
            @Nullable final Integer numDocs,
            @Nullable final SchedulerCallback execTimeCallback,
            @Nullable final SchedulerCallback waitTimeCallback
    ) {
        this.userName = userName;
        this.clientName = clientName;
        this.priority = priority;
        this.taskThread = Thread.currentThread();
        this.requestContext = RequestContext.THREAD_REQUEST_CONTEXT.get();
        this.dataset = dataset;
        this.shardName = shardName;
        this.numDocs = numDocs;
        this.taskId = nextTaskId.incrementAndGet();
        creationTimestamp = System.nanoTime();
        this.session = session;

        this.schedulerExecTimeCallback = execTimeCallback;
        this.schedulerWaitTimeCallback = waitTimeCallback;
    }

    private ImhotepTask(AbstractImhotepMultiSession session) {
        this(session.getUserName(), session.getClientName(), session.getPriority(), session, null, null, null,
                (schedulerType, execTime) -> session.schedulerExecTimeCallback(schedulerType, execTime),
                (schedulerType, waitTime) -> session.schedulerWaitTimeCallback(schedulerType, waitTime));
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
        final long nanoTime = System.nanoTime();
        final long waitTime = nanoTime - lastWaitStartTime;
        if (schedulerWaitTimeCallback != null) {
            schedulerWaitTimeCallback.call(schedulerType, waitTime);
        }
        if (requestContext != null) {
            requestContext.schedulerWaitTimeCallback(schedulerType, waitTime);
        }
        waitLock.countDown();
        synchronized (executionTimeStatsLock) {
            lastExecutionStartTime = nanoTime;
        }
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
                if (session != null && session.isClosed()) {
                    throw new InvalidSessionException("Session with id " + session.getSessionId() + " was already closed");
                }
                synchronized (executionTimeStatsLock) {
                    nextYieldTime = ownerScheduler.getCurrentTimeMillis() + ownerScheduler.getExecutionChunkMillis();
                }
            } catch(InterruptedException ignored){ }
        }
    }

    /** returns the task resource consumption */
    synchronized long stopped(SchedulerType schedulerType) {
        long executionTime;
        synchronized (executionTimeStatsLock) {
            if (!isRunning()) {
                throw new IllegalStateException("Tried to finish a task that wasn't started");
            }
            executionTime = System.nanoTime() - lastExecutionStartTime;
            totalExecutionTime += executionTime;
            lastExecutionStartTime = 0;
            nextYieldTime = 0;
        }
        if (schedulerExecTimeCallback != null) {
            schedulerExecTimeCallback.call(schedulerType, executionTime);
        }
        if (requestContext != null) {
            requestContext.schedulerExecTimeCallback(schedulerType, executionTime);
        }
        ownerScheduler = null;
        return executionTime;
    }

    @Override
    public String toString() {
        String taskStringVal ="ImhotepTask{" +
                "taskId=" + taskId +
                ", userName='" + userName + '\'' +
                ", clientName='" + clientName + '\'' +
                ", priority='" + priority + '\'';
        if (session != null) {
            taskStringVal += ", sessionID='" + session.getSessionId() + '\'';
        }

        // innerSession access is dangerous
        // It must be ensured that any methods that are called from here are
        // non-synchronized methods
        final String shardPath = Optional.ofNullable(innerSession)
                .map(AbstractImhotepSession::getShardPath)
                .map(Object::toString)
                .orElse(null);
        if (shardPath != null) {
            taskStringVal += ", shardPath='" + shardPath + '\'';
        }

        if (requestContext != null) {
            taskStringVal += ", requestType=" + requestContext.getRequestType();
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

    public long getNextYieldTime() {
        return nextYieldTime;
    }

    @Nullable
    public AbstractImhotepMultiSession getSession() {
        return session;
    }

    public String getUserName() {
        return userName;
    }

    public String getClientName() {
        return clientName;
    }

    public byte getPriority() {
        return priority;
    }

    /**
     * Returns empty iff this task is not currently running
     */
    public OptionalLong getCurrentExecutionTime() {
        synchronized (executionTimeStatsLock) {
            if (!isRunning()) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(System.nanoTime() - lastExecutionStartTime);
        }
    }

    public StackTraceElement[] getStackTrace() {
        return taskThread.getStackTrace();
    }

    public TaskSnapshot getSnapshot(final boolean takeStackTrace) {
        @Nullable StackTraceElement[] stackTrace = null;
        if (takeStackTrace) {
            try {
                stackTrace = getStackTrace();
            } catch (final Exception e) {
                LOGGER.warn("Failed to take stack trace", e);
            }
        }
        synchronized (executionTimeStatsLock) {
            return new TaskSnapshot(
                    this.taskId,
                    this.session,
                    this.innerSession,
                    this.requestContext,
                    this.creationTimestamp,
                    this.userName,
                    this.clientName,
                    this.dataset,
                    this.shardName,
                    this.numDocs,
                    this.priority,
                    this.lastExecutionStartTime,
                    this.lastWaitStartTime,
                    this.totalExecutionTime,
                    stackTrace
            );
        }
    }

    private interface SchedulerCallback {
        void call(SchedulerType schedulerType, long time);
    }

    // pretend that task has started some time before the actual start.
    // used in tests to emulate actual work and not do Thread.sleep()
    @VisibleForTesting
    public void changeTaskStartTime(final long decreaseStartTimeMillis) {
        synchronized (executionTimeStatsLock) {
            this.lastExecutionStartTime -= TimeUnit.MILLISECONDS.toNanos(decreaseStartTimeMillis);
            this.nextYieldTime -= decreaseStartTimeMillis;
        }
    }
}
